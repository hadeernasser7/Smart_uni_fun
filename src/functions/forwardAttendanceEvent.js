/**
 * ===================================================================================
 * @file      forwardAttendanceEvent.js
 * @desc      Azure Event Hub Trigger function utilizing the Node.js Programming Model v4.
 *            Listens to telemetry streams on the built-in Event Hub, routes payloads
 *            by 'type', and POSTs them to the backend with secure HMAC authorization.
 * ===================================================================================
 */
const { app } = require("@azure/functions");

app.eventHub("forwardAttendanceEvent", {
    connection: "EventHubConnection", // Resolves to App Setting
    eventHubName: "%EventHubName%",    // Resolves to App Setting via %% syntax
    consumerGroup: "smart-uni-func",
    handler: async (messages, context) => {
        // Handle single event telemetry or batched event streams gracefully
        const events = Array.isArray(messages) ? messages : [messages];

        for (const event of events) {
            context.log("Received IoT Telemetry Event:", JSON.stringify(event));

            const eventType = event.type;
            if (!eventType) {
                context.log("Skipping event: missing 'type' payload field.");
                continue;
            }

            let endpoint;

            switch (eventType) {
                case "enrollment":
                    // GUARD: Skip enrollment failures early to prevent backend registration errors.
                    if (event.success === false) {
                        context.log(`Enrollment failed on device ${event.deviceId}: ${event.error || "unknown_error"}. Skipping register.`);
                        continue;
                    }
                    endpoint = "/api/v1/attendance/fingerprints/register";
                    break;
                case "attendance":
                    endpoint = "/api/v1/attendance/fingerprint-mark";
                    break;
                case "heartbeat":
                    endpoint = "/api/v1/attendance/devices/heartbeat";
                    break;
                default:
                    context.log("Skipping event: unsupported 'type' field: ", eventType);
                    continue;
            }

            // Clean up base URL slash configurations and combine paths safely
            const backendUrl = process.env.BACKEND_API_URL.replace(/\/$/, "") + endpoint;

            try {
                context.log(`Forwarding ${eventType} event from ${event.deviceId} to ${backendUrl}`);
                const response = await fetch(backendUrl, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        "x-device-secret": process.env.IOT_DEVICE_SECRET,
                    },
                    body: JSON.stringify(event),
                });

                if (!response.ok) {
                    const errorText = await response.text();
                    context.log(`Backend error details: Status ${response.status} | Msg: ${errorText}`);
                    // Throwing forces Event Hub retry loop if endpoint experiences temporary failure (5xx)
                    if (response.status >= 500) {
                        throw new Error(`Transient backend error returned status ${response.status}`);
                    }
                } else {
                    context.log(`Successfully forwarded ${eventType} telemetry to backend.`);
                }
            } catch (err) {
                context.log("Network transmission or forwarding failure:", err.message);
                // Propagate exception to trigger Event Hub backoff retry mechanism
                throw err;
            }
        }
    },
});