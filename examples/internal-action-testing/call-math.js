/* eslint-disable no-console */
"use strict";

let ServiceBroker = require("../../src/service-broker");

// let transporter = process.env.TRANSPORTER || "TCP";
let transporter = "nats://localhost:4222";

// Create broker
let broker = new ServiceBroker({
	namespace: "multi",
	nodeID: process.argv[2] || "call-" + process.pid,
	transporter,
	logger: console,
	logLevel: process.env.LOGLEVEL,
	logFormatter: "simple"
});

broker.start()
	.then(() => {
		setInterval(() => {
			return broker.call("math.add", { a: 1, b: 1 })
				.then(console.log)
				.catch(console.error);
		}, 1000);
	});
