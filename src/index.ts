#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from "@modelcontextprotocol/sdk/types.js";

const server = new Server(
  {
    name: "schema-transformer",
    version: "0.1.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "generate_sql",
      description: "Generates a SQL transformation query based on the provided schema.",
      inputSchema: {
        type: "object",
        properties: {
          schema: {
            type: "object",
            description: "The table schema containing field definitions.",
            properties: {
              fields: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    name: { type: "string" },
                    type: { type: "string" },
                  },
                  required: ["name", "type"],
                },
              },
            },
            required: ["fields"],
          },
          table_name: {
            type: "string",
            description: "The name of the input table to transform.",
          },
          output_table_name: {
            type: "string",
            description: "The name of the final output table.",
          },
        },
        required: ["schema", "table_name", "output_table_name"],
      },
    },
  ],
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (request.params.name !== "generate_sql") {
    throw new McpError(ErrorCode.MethodNotFound, `Unknown tool: ${request.params.name}`);
  }

  const { schema, table_name, output_table_name } = request.params.arguments as {
    schema: { fields: { name: string; type: string }[] };
    table_name: string;
    output_table_name: string;
  };

  if (!schema || !schema.fields) {
    throw new McpError(ErrorCode.InvalidParams, "Invalid schema format");
  }

  // Mapping input schema fields to output schema transformations
  const fieldMappings: Record<string, string> = {
    Date: "PARSE_DATETIME('%d %b %Y, %H:%M', Date) AS PSP_Date",
    UTC: "UTC AS PSP_TransactionID",
    Transaction_Type: "Transaction_Type AS PSP_Currency",
    Status: "Status AS PSP_Status",
    Commission: "COMMISSION AS PSP_OriginalFee",
    Fee_Currency: "Fee_Currency AS PSP_OriginalFeeCurrency",
    Pays_Fee: "Pays_Fee",
    Debit: "CASE WHEN Pays_Fee = 'Sender' THEN Credit * -1 ELSE Debit * 1 END AS PSP_Amount",
    Credit: "CASE WHEN Pays_Fee = 'Receiver' THEN Debit * 1 ELSE Credit * -1 END AS PSP_Amount",
    Direction: "CASE WHEN Pays_Fee = 'Sender' THEN 'Withdrawal' ELSE 'Deposit' END AS PSP_Type",
    Note: "REGEXP_EXTRACT(Note, r'([A-Z]+[0-9]+)') AS PSP_LoginID, REGEXP_EXTRACT(Note, r'-(\\d+)$') AS PSP_TraceID",
    API_CSI: "API_CSI",
    Sender: "Sender",
    Currency_Deposit: "Currency_Deposit",
    Receiver: "Receiver",
    Currency_Withdrawal: "Currency_Withdrawal",
    Transaction_information: "Transaction_information"
  };

  // Construct SQL transformation logic dynamically
  const selectFields = Object.entries(fieldMappings)
    .map(([inputField, transformation]) => `  ${transformation}`)
    .join(",\n");

  const sqlQuery = `
  -- Start Transaction
  BEGIN TRANSACTION;

  -- Step 1: Deduplicate and prepare raw data
  CREATE OR REPLACE TEMP TABLE ${table_name}_cleaned AS
  SELECT DISTINCT * FROM \`${table_name}\`;

  -- Step 2: Transform data according to new schema
  CREATE OR REPLACE TEMP TABLE ${table_name}_transformed AS
  SELECT
${selectFields}
  FROM ${table_name}_cleaned;

  -- Step 3: Final Table with Additional Computed Columns
  CREATE OR REPLACE TEMP TABLE Final_Table AS
  SELECT *,
    CASE WHEN PSP_Amount > 0 THEN 'Debit' ELSE 'Credit' END AS PSP_DrCr,
    CASE
      WHEN EXTRACT(DAY FROM PSP_Date) > 21 THEN 'Week 4'
      WHEN EXTRACT(DAY FROM PSP_Date) > 14 THEN 'Week 3'
      WHEN EXTRACT(DAY FROM PSP_Date) > 7 THEN 'Week 2'
      ELSE 'Week 1'
    END AS PSP_Week
  FROM ${table_name}_transformed;

  -- Step 4: Write Final Data to Output Table
  CREATE OR REPLACE TABLE \`${output_table_name}\` AS
  SELECT * FROM Final_Table;

  -- Commit transaction
  COMMIT TRANSACTION;
  `;

  return {
    content: [
      {
        type: "text",
        text: sqlQuery,
      },
    ],
  };
});

// Start the MCP server
const transport = new StdioServerTransport();
server.connect(transport).catch(console.error);
