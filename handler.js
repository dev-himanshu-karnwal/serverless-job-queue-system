import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { nanoid } from "nanoid";

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

const sqs = new SQSClient({ region: "ap-south-1" });
const db = DynamoDBDocumentClient.from(
  new DynamoDBClient({ region: "ap-south-1" }),
);

const QUEUE_URL = process.env.QUEUE_URL;

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
      await db.send(new UpdateCommand({
        TableName: "jobs",
        Key: { jobId },
        UpdateExpression: "SET #s = :status",
        ExpressionAttributeNames: { "#s": "status" },
        ExpressionAttributeValues: { ":status": "processing" }
      }));

      // 2. simulate work
      await new Promise(resolve => setTimeout(resolve, 2000));

      // 3. mark as completed
      await db.send(new UpdateCommand({
        TableName: "jobs",
        Key: { jobId },
        UpdateExpression: "SET #s = :status, #r = :res",
        ExpressionAttributeNames: { "#s": "status", "#r": "result" },
        ExpressionAttributeValues: {
          ":status": "completed",
          ":res": "processed successfully"
        }
      }));

      console.log(`Job ${jobId} completed`);

    } catch (err) {
      console.error("Worker error:", err);

      // mark as failed
      await db.send(new UpdateCommand({
        TableName: "jobs",
        Key: { jobId },
        UpdateExpression: "SET #s = :status",
        ExpressionAttributeNames: { "#s": "status" },
        ExpressionAttributeValues: { ":status": "failed" }
      }));

      throw err; // important → triggers retry
    }
  }

}