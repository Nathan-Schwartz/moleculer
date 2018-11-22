"use strict";

let ServiceBroker = require("../../src/service-broker");
let mathService = require("./math-service");

// let transporter = process.env.TRANSPORTER || "TCP";
let transporter = "nats://localhost:4222";

// Create broker
let broker = new ServiceBroker({
	namespace: "multi",
	nodeID: process.argv[2] || "host-" + process.pid,
	transporter,
	logger: console,
	logLevel: process.env.LOGLEVEL,
	logFormatter: "simple"
});

broker.createService(mathService('public'));

broker.start();
