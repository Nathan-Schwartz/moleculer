"use strict";

let ServiceBroker = require("../../src/service-broker");
let mathService = require("./math-service");

// let transporter = process.env.TRANSPORTER || "TCP";
let transporter = "nats://localhost:4222";

// Create broker
let broker = new ServiceBroker({
	namespace: "multi",
	nodeID: process.argv[2] || "internal-call-" + process.pid,
	transporter,
	logger: console,
	logLevel: process.env.LOGLEVEL,
	logFormatter: "simple"
});




broker.createService({
	mixins: [mathService("private")],
	actions: {
		add: {
			visibility: "protected",
		}
	}
});

// {
// 	name: "math",
// 	actions: {
// 		add: {
// 			visibility: "protected",
// 			handler(ctx) {
// 				return {
// 					sum: Number(ctx.params.a) + Number(ctx.params.b),
// 					handler: "server",
// 				};
// 			},
// 		}
// 	},

// }

broker.start()
	.then(() => {
		setInterval(() => {
			return broker.call("math.add", { a: 1, b: 1 })
				.then(console.log)
				.catch(console.error);
		}, 1000);
	});
