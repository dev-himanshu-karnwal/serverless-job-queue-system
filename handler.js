import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import { nanoid } from "nanoid";

const QUEUE_URL = process.env.QUEUE_URL;
const sqs = new SQSClient({ region: "ap-south-1" });
const db = DynamoDBDocumentClient.from(
  new DynamoDBClient({ region: "ap-south-1" }),
);

export const hello = async (event) => {
  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      message: "Go Serverless v1.0! Your function executed successfully!",
    }),
  };
};

export const createJob = async (event) => {
  try {
    const body = JSON.parse(event.body || "{}");

    const jobId = nanoid();

    // 1. store job
    await db.send(new PutCommand({
      TableName: `jobs-${process.env.STAGE || "dev"}`,
      Item: {
        jobId,
        status: "pending",
        createdAt: Date.now(),
        retryCount: 0
      }
    }));

    // 2. push to SQS
    await sqs.send(
      new SendMessageCommand({
        QueueUrl: QUEUE_URL,
        MessageBody: JSON.stringify({
          jobId,
          payload: body,
        }),
      }),
    );

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ jobId }),
    };
  } catch (err) {
    console.error("ERROR:", err);
    return {
      statusCode: 500,
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        error: "failed to create job",
      }),
    };
  }
};

export const worker = async (event) => {
  console.log(JSON.stringify({ event }));

  for (const record of event.Records) {
    const body = JSON.parse(record.body);
    const jobId = body.jobId;

    try {
      // Step 1: mark processing (idempotent)
      await db.send(new UpdateCommand({
        TableName: `jobs-${process.env.STAGE || "dev"}`,
        Key: { jobId },
        UpdateExpression: "SET #s = :processing ADD retryCount :inc",
        ConditionExpression: "#s <> :completed",
        ExpressionAttributeNames: { "#s": "status" },
        ExpressionAttributeValues: {
          ":processing": "processing",
          ":completed": "completed",
          ":inc": 1
        }
      }));

      // Step 2: simulate work
      await new Promise((resolve) => setTimeout(resolve, 3000));

      // Step 3: mark completed
      await db.send(new UpdateCommand({
        TableName: `jobs-${process.env.STAGE || "dev"}`,
        Key: { jobId },
        UpdateExpression: "SET #s = :status, #r = :res, processedAt = :time",
        ExpressionAttributeNames: {
          "#s": "status",
          "#r": "result"
        },
        ExpressionAttributeValues: {
          ":status": "completed",
          ":res": "processed successfully",
          ":time": Date.now()
        }
      }));

      console.log(JSON.stringify({
        jobId,
        status: "completed"
      }));

    } catch (err) {

      if (err.name === "ConditionalCheckFailedException") {
        console.log(JSON.stringify({
          jobId,
          message: "Already processed"
        }));
        continue;
      }

      console.error(JSON.stringify({
        jobId,
        error: err.message
      }));

      // mark failed
      await db.send(new UpdateCommand({
        TableName: `jobs-${process.env.STAGE || "dev"}`,
        Key: { jobId },
        UpdateExpression: "SET #s = :status, lastError = :err",
        ExpressionAttributeNames: { "#s": "status" },
        ExpressionAttributeValues: {
          ":status": "failed",
          ":err": err.message
        }
      }));

      throw err;
    }
  }
};

export const getJob = async (event) => {
  try {
    const jobId = event.pathParameters.id;
    const job = await db.send(new GetCommand({
      TableName: `jobs-${process.env.STAGE || "dev"}`,
      Key: { jobId }
    }));

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(job.Item),
    };
  } catch (err) {
    console.error("ERROR:", err);
    return {
      statusCode: 500,
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ error: "failed to get job" }),
    };
  }
};
