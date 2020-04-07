/*
 * moleculer
 * Copyright (c) 2018 MoleculerJS (https://github.com/moleculerjs/moleculer)
 * MIT Licensed
 */

"use strict";

const url = require("url");
const Transporter 	= require("./base");
const { isPromise }	= require("../utils");

const {
	PACKET_REQUEST,
	PACKET_RESPONSE,
	PACKET_UNKNOWN,
	PACKET_EVENT,
	PACKET_DISCOVER,
	PACKET_INFO,
	PACKET_DISCONNECT,
	PACKET_HEARTBEAT,
	PACKET_PING,
	PACKET_PONG,
} = require("../packets");

/**
 * Transporter for AMQP
 *
 * More info: https://www.amqp.org/
 *
 * For test:
 *
 * 	 docker run -d -p 5672:5672 -p 15672:15672 --name rabbit rabbitmq:3-management
 *
 * @class AmqpTransporter
 * @extends {Transporter}
 */
class AmqpTransporter extends Transporter {

	/**
	 * Creates an instance of AmqpTransporter.
	 *
	 * @param {any} opts
	 *
	 * @memberof AmqpTransporter
	 */
	constructor(opts) {
		if (typeof opts === "string")
			opts = { url: opts };
		else if (opts == null)
			opts = {};

		// Number of requests a broker will handle concurrently
		if (typeof opts.prefetch !== "number")
			opts.prefetch = 1;

		// Number of milliseconds before an event expires
		if (typeof opts.eventTimeToLive !== "number")
			opts.eventTimeToLive = null;

		if (typeof opts.heartbeatTimeToLive !== "number")
			opts.heartbeatTimeToLive = null;

		if (typeof opts.queueOptions !== "object")
			opts.queueOptions = {};

		if (typeof opts.exchangeOptions !== "object")
			opts.exchangeOptions = {};

		if (typeof opts.messageOptions !== "object")
			opts.messageOptions = {};

		if (typeof opts.consumeOptions !== "object")
			opts.consumeOptions = {};

		// The default behavior is to delete the queues after they haven't had any
		// connected consumers for 2 minutes.
		const autoDeleteQueuesAfterDefault = 2*60*1000;

		opts.autoDeleteQueues = opts.autoDeleteQueues === true
			? autoDeleteQueuesAfterDefault
			: typeof opts.autoDeleteQueues === "number"
				? opts.autoDeleteQueues
				: opts.autoDeleteQueues === false
					? -1
					: autoDeleteQueuesAfterDefault;

		// Support for multiple URLs (clusters)
		opts.url = Array.isArray(opts.url)
			? opts.url
			: !opts.url
				? [""]
				: opts.url.split(";").filter(s => !!s);

		super(opts);

		this.hasBuiltInBalancer = true;
		this.topology = {};
		this.reconnectCount = 0;
	}

	/**
	 * Connect to a AMQP server
	 *
	 * @memberof AmqpTransporter
	 */
	connect() {
		// This promise won't reject, but will take an indefinite amount of time to resolve.
		return new this.broker.Promise((resolve) => {
			let amqp;
			try {
				amqp = require("amqp-connection-manager");
			} catch(err) {
				/* istanbul ignore next */
				this.broker.fatal("The 'amqp-connection-manager' or 'amqp' package is missing. Please install it with 'npm install amqp-connection-manager amqplib --save' command.", err, true);
			}

			// Creates a connection manager and event handlers to handle reconnections. It can be safely interacted with regardless of connection state.
			this.connection = amqp.connect(this.opts.url, {
				connectionOptions: Object.assign({},
					(this.opts.socketOptions || {}),
					{ servername: url.parse(this.opts.url[0]).hostname }
				)
			})
				.on("connect", ({ url: brokerUrl }) => {
					this.connected = true;
					this.logger.info(`AMQP is connected to broker at ${url.parse(brokerUrl).host} (reconnectCount=${this.reconnectCount})`);

					const isReconnect = this.reconnectCount !== 0;
					this.reconnectCount += 1;

					return this.onConnected(isReconnect).then(() => resolve());
				})
				.on("disconnect", ({ err }) => {
					if (err) this.logger.error("AMQP connection error.", err);
					this.connected = false;

					if (this.transit.disconnecting) {
						this.logger.info("AMQP connection is closed gracefully.");
					} else {
						this.logger.error("AMQP connection is closed.");
					}
				})

				.on("blocked", (reason) => {
					this.logger.warn("AMQP connection is blocked.", reason);
				})
				.on("unblocked", () => {
					this.logger.info("AMQP connection is unblocked.");
				});

			// Creates a channel wrapper with a prefetch. The setup function is called whenever a connection is established,
			// so topology and subscriptions are preserved upon broker crashes.
			//
			// We will add to the setup in our subscription methods.
			this.channel = this.connection
				.createChannel({
					setup: (channel) => {
						this.logger.info(`AMQP channel is created. (prefetch=${this.opts.prefetch})`);
						channel.prefetch(this.opts.prefetch);
					}
				})
				.on("close", () => {
					// No need to reject here since close event on connection will handle reconnection logic.
					if (this.transit.disconnecting)
						this.logger.info("AMQP channel is closed gracefully.");
					else
						this.logger.warn("AMQP channel is closed.");
				})
				.on("error", (err) => {
					this.logger.error("AMQP channel error.", err);
				})
				.on("connect", () => {
					this.logger.warn("AMQP channel connected.");
				});
		});
	}

	/**
	 * Disconnect from an AMQP server
	 *
	 * @memberof AmqpTransporter
	 * @description Close the connection and unbind this node's queues.
	 * This prevents messages from being broadcasted to a dead node.
	 * Note: Some methods of ending a node process don't allow disconnect to fire, meaning that
	 * some dead nodes could still receive published packets.
	 * Queues and Exchanges are not be deleted since they could contain important messages.
	 */
	disconnect() {
		// removeSetup ensures that the channel stops asserting the topology, and will run our teardown functions if a connection exists.
		// If a connection doesn't exist, teardown functions won't run and we will skip the unbinding step.
		return this.broker.Promise.all(Object.keys(this.topology)
			.map(key => this.channel.removeSetup(this.topology[key].setup, this.topology[key].teardown)))
			.then(() => this.channel.close())
			.then(() => this.connection.close())
			.catch(err => this.logger.warn(err));
	}

	/**
	 * Get assertQueue options by packet type.
	 *
	 * @param {String} packetType
	 *
	 * @memberof AmqpTransporter
	 */
	_getQueueOptions(packetType, balancedQueue) {
		let packetOptions;
		switch(packetType) {
			// Requests and responses don't expire.
			case PACKET_REQUEST:
				packetOptions = this.opts.autoDeleteQueues >= 0 && !balancedQueue
					? { expires: this.opts.autoDeleteQueues }
					: {};
				break;
			case PACKET_RESPONSE:
				packetOptions = this.opts.autoDeleteQueues >= 0
					? { expires: this.opts.autoDeleteQueues }
					: {};
				break;

			// Consumers can decide how long events live
			// Load-balanced/grouped events
			case PACKET_EVENT + "LB":
			case PACKET_EVENT:
				packetOptions = this.opts.autoDeleteQueues >= 0
					? { expires: this.opts.autoDeleteQueues }
					: {};
				// If eventTimeToLive is specified, add to options.
				if (this.opts.eventTimeToLive)
					packetOptions.messageTtl = this.opts.eventTimeToLive;
				break;

			// Packet types meant for internal use
			case PACKET_HEARTBEAT:
				packetOptions = { autoDelete: true };
				// If heartbeatTimeToLive is specified, add to options.
				if (this.opts.heartbeatTimeToLive)
					packetOptions.messageTtl = this.opts.heartbeatTimeToLive;
				break;
			case PACKET_DISCOVER:
			case PACKET_DISCONNECT:
			case PACKET_UNKNOWN:
			case PACKET_INFO:
			case PACKET_PING:
			case PACKET_PONG:
				packetOptions = { autoDelete: true };
				break;
		}

		return Object.assign(packetOptions, this.opts.queueOptions);
	}

	/**
	 * Build a function to handle requests.
	 *
	 * @param {String} cmd
	 * @param {Boolean} needAck
	 *
	 * @memberof AmqpTransporter
	 */
	_consumeCB(cmd, needAck = false) {
		return (msg) => {
			const result = this.receive(cmd, msg.content);

			// If a promise is returned, acknowledge the message after it has resolved.
			// This means that if a worker dies after receiving a message but before responding, the
			// message won't be lost and it can be retried.
			//
			// Calling channel.ack is safe, even if disconnected
			if(needAck) {
				if (isPromise(result)) {
					return result
						.then(() => {
							this.channel.ack(msg);
						})
						.catch(err => {
							this.logger.error("Message handling error.", err);
							this.channel.ack(msg);
						});
				} else {
					this.channel.ack(msg);
				}
			}

			return result;
		};
	}


	/**
	 * Subscribe to a command
	 *
	 * @param {String} cmd
	 * @param {String} nodeID
	 *
	 * @memberof AmqpTransporter
	 * @description Initialize queues and exchanges for all packet types except Request.
	 *
	 * All packets that should reach multiple nodes have a dedicated queue per node, and a single
	 * exchange that routes each message to all queues. These packet types will not use
	 * acknowledgements and have a set time-to-live. The time-to-live for EVENT packets can be
	 * configured in options.
	 * Examples: INFO (sometimes), DISCOVER, DISCONNECT, HEARTBEAT, PING, PONG, EVENT
	 *
	 * Other Packets are headed towards a specific node or queue. These don't need exchanges and
	 * packets of this type will not expire.
	 * Examples: REQUEST, RESPONSE
	 *
	 * RESPONSE: Each node has its own dedicated queue and acknowledgements will not be used.
	 *
	 * REQUEST: Each action has its own dedicated queue. This way if an action has multiple workers,
	 * they can all pull from the same queue. This allows a message to be retried by a different node
	 * if one dies before responding.
	 *
	 * Note: Queue's for REQUEST packet types are not initialized in the subscribe method because the
	 * actions themselves are not available from within the method. Instead they are intercepted from
	 * "prefix.INFO" packets because they are broadcast whenever a service is registered.
	 *
	 */
	subscribe(cmd, nodeID) {
		const topic = this.getTopicName(cmd, nodeID);

		// Some topics are specific to this node already, in these cases we don't need an exchange.
		if (nodeID != null) {
			const needAck = [PACKET_REQUEST, PACKET_EVENT].indexOf(cmd) !== -1;

			this.topology[topic] = {
				setup: (channel) =>
					channel.assertQueue(topic, this._getQueueOptions(cmd))
						.then(() => channel.consume(
							topic,
							this._consumeCB(cmd, needAck),
							Object.assign({ noAck: !needAck }, this.opts.consumeOptions)
						)),
				teardown: this.broker.Promise.resolve,
			};
			return this.channel.addSetup(this.topology[topic].setup);
		} else {
			// Create a queue specific to this nodeID so that this node can receive broadcasted messages.
			const queueName = `${this.prefix}.${cmd}.${this.nodeID}`;
			const bindingArgs = [queueName, topic, ""];
			this.topology[queueName] = {
				setup: (channel) => {
					return this.broker.Promise.all([
						channel.assertExchange(topic, "fanout", this.opts.exchangeOptions),
						channel.assertQueue(queueName, this._getQueueOptions(cmd)),
					])
						.then(() => this.broker.Promise.all([
							channel.bindQueue(...bindingArgs),
							channel.consume(
								queueName,
								this._consumeCB(cmd),
								Object.assign({ noAck: true }, this.opts.consumeOptions)
							)
						]));

				},
				teardown: (channel) => channel.unbindQueue(...bindingArgs),
			};

			return this.channel.addSetup(this.topology[queueName].setup);
		}
	}

	/**
	 * Subscribe to balanced action commands
	 *
	 * @param {String} action
	 * @memberof AmqpTransporter
	 */
	subscribeBalancedRequest(action) {
		const queue = `${this.prefix}.${PACKET_REQUEST}B.${action}`;

		this.topology[queue] = {
			setup: (channel) => {
				return channel.assertQueue(queue, this._getQueueOptions(PACKET_REQUEST, true))
					.then(() => channel.consume(
						queue,
						this._consumeCB(PACKET_REQUEST, true),
						this.opts.consumeOptions
					));
			},
			teardown: this.broker.Promise.resolve,
		};

		return this.channel.addSetup(this.topology[queue].setup);
	}

	/**
	 * Subscribe to balanced event command
	 *
	 * @param {String} event
	 * @param {String} group
	 * @memberof AmqpTransporter
	 */
	subscribeBalancedEvent(event, group) {
		const queue = `${this.prefix}.${PACKET_EVENT}B.${group}.${event}`;

		this.topology[queue] = {
			setup: (channel) => {
				return channel.assertQueue(queue, this._getQueueOptions(PACKET_EVENT + "LB", true))
					.then(() => channel.consume(
						queue,
						this._consumeCB(PACKET_EVENT, true),
						this.opts.consumeOptions
					));
			},
			teardown: this.broker.Promise.resolve,
		};
		return this.channel.addSetup(this.topology[queue].setup);
	}

	/**
	 * Send data buffer.
	 *
	 * @param {String} topic
	 * @param {Buffer} data
	 * @param {Object} meta
	 *
	 * @returns {Promise}
	 */
	send(topic, data, { balanced, packet }) {
		// Messages are queued in memory when not connected to a broker. Queueing heartbeat packets isn't useful, but other packeets could succeed upon reconnection.
		if (!this.connection.isConnected() && packet.type === PACKET_HEARTBEAT) {
			return this.broker.Promise.resolve();
		}

		if (packet.target != null || balanced) {
			return this.channel.sendToQueue(topic, data, this.opts.messageOptions);
		} else {
			return this.channel.publish(topic, "", data, this.opts.messageOptions);
		}
	}
}

module.exports = AmqpTransporter;
