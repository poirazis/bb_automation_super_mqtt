import { AutomationStepInputBase } from "@budibase/types";
import * as mqtt from "mqtt";

export default async function run({
  inputs,
}: AutomationStepInputBase & { inputs: Record<string, any> }) {
  try {
    const { host, port, protocol, username, password, topic, message } = inputs;

    // Validate required inputs
    if (!host || !port || !protocol || !topic || !message) {
      return {
        success: false,
        message:
          "Missing required inputs: host, port, protocol, topic, and message are required",
      };
    }

    // Validate protocol
    if (protocol !== "tcp" && protocol !== "ws") {
      return {
        success: false,
        message: "Protocol must be either 'tcp' or 'ws'",
      };
    }

    // Construct the connection URL based on protocol
    const url = `${protocol}://${host}:${port}`;

    // Connection options
    const connectOptions: mqtt.IClientOptions = {
      clientId: `budibase_mqtt_${Date.now()}`,
      clean: true,
      reconnectPeriod: 1000,
      connectTimeout: 30 * 1000,
    };

    // Add authentication if provided
    if (username) {
      connectOptions.username = username;
    }
    if (password) {
      connectOptions.password = password;
    }

    // Create MQTT client and establish connection
    const client = mqtt.connect(url, connectOptions);

    // Return a promise that resolves when publish is complete
    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        client.end();
        resolve({
          success: false,
          message: "MQTT connection timeout after 30 seconds",
        });
      }, 30000); // 30 second timeout

      client.on("connect", () => {
        console.log(`Connected to MQTT broker at ${url}`);

        // Publish the message
        client.publish(topic, message, { qos: 1 }, (err: any) => {
          clearTimeout(timeout);

          if (err) {
            client.end();
            resolve({
              success: false,
              message: `Failed to publish message: ${err.message}`,
            });
          } else {
            console.log(`Published message to topic: ${topic}`);
            client.end(() => {
              resolve({
                success: true,
                message: `Message published successfully to topic: ${topic}`,
              });
            });
          }
        });
      });

      client.on("error", (err: any) => {
        clearTimeout(timeout);
        client.end();
        resolve({
          success: false,
          message: `Connection error: ${err.message}`,
        });
      });
    });
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : "Unknown error occurred";
    return {
      success: false,
      message: `Automation error: ${errorMessage}`,
    };
  }
}
