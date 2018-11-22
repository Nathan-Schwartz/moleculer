module.exports = (handler) => ({
	name: "math",
	actions: {
		add: {
			handler(ctx) {
				return {
					sum: Number(ctx.params.a) + Number(ctx.params.b),
					handler,
				};
			},
		}
	},

});
