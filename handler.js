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
    await db.send(
      new PutCommand({
        TableName: "jobs",
        Item: {
          jobId,
          status: "pending",
          createdAt: Date.now(),
        },
      }),
    );

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
  console.log("EVENT:", JSON.stringify(event, null, 2));

  for (const record of event.Records) {
    const body = JSON.parse(record.body);
    const jobId = body.jobId;

    try {
      // 1. mark as processing
      await db.send(
        new UpdateCommand({
          TableName: "jobs",
          Key: { jobId },
          UpdateExpression: "SET #s = :processing",
          ConditionExpression: "#s <> :completed",
          ExpressionAttributeNames: { "#s": "status" },
          ExpressionAttributeValues: {
            ":processing": "processing",
            ":completed": "completed"
          },
        })
      );

      // 2. simulate work
      await new Promise((resolve) => setTimeout(resolve, 10000));

      // 3. mark as completed
      await db.send(
        new UpdateCommand({
          TableName: "jobs",
          Key: { jobId },
          UpdateExpression: "SET #s = :status, #r = :res",
          ConditionExpression: "attribute_not_exists(#s) OR #s <> :completed",
          ExpressionAttributeNames: { "#s": "status", "#r": "result" },
          ExpressionAttributeValues: {
            ":status": "completed",
            ":res": "processed successfully",
            ":completed": "completed",
          },
        }),
      );

      console.log(`Job ${jobId} completed`);
    } catch (err) {
      console.error("Worker error:", err);

      if (err.name === "ConditionalCheckFailedException") {
        console.log("Already completed, skipping safely");
        continue;
      }

      // mark as failed
      await db.send(
        new UpdateCommand({
          TableName: "jobs",
          Key: { jobId },
          UpdateExpression: "SET #s = :status",
          ExpressionAttributeNames: { "#s": "status" },
          ExpressionAttributeValues: { ":status": "failed" },
        }),
      );

      throw err; // important → triggers retry
    }
  }
};

export const getJob = async (event) => {
  try {
    const jobId = event.pathParameters.id;
    const job = await db.send(
      new GetCommand({
        TableName: "jobs",
        Key: { jobId },
      }),
    );

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
