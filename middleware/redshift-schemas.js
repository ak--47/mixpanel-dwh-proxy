const eventsSchema = [
	{
		"name": "event",
		"type": "VARCHAR"		
	},
	{
		"name": "event_time",
		"type": "TIMESTAMP"		
	},
	{
		"name": "insert_time",
		"type": "TIMESTAMP"		
	},
	{
		"name": "token",
		"type": "VARCHAR"		
	},
	{
		"name": "device_id",
		"type": "VARCHAR"		
	},
	{
		"name": "insert_id",
		"type": "VARCHAR"		
	},
	{
		"name": "user_id",
		"type": "VARCHAR"		
	},
	{
		"name": "distinct_id",
		"type": "VARCHAR"		
	},
	{
		"name": "properties",
		"type": "SUPER"		
	}
];

const usersSchema = [
	{
		"name": "token",
		"type": "VARCHAR"		
	},
	{
		"name": "distinct_id",
		"type": "VARCHAR"		
	},
	{
		"name": "ip",
		"type": "VARCHAR"		
	},
	{
		"name": "insert_time",
		"type": "TIMESTAMP"		
	},
	{
		"name": "operation",
		"type": "VARCHAR"		
	},
	{
		"name": "properties",
		"type": "SUPER"		
	}
];


const groupsSchema = [
	{
		"name": "token",
		"type": "VARCHAR"		
	},
	{
		"name": "group_key",
		"type": "VARCHAR"		
	},
	{
		"name": "group_id",
		"type": "VARCHAR"		
	},
	{
		"name": "operation",
		"type": "VARCHAR"		
	},
	{
		"name": "insert_time",
		"type": "TIMESTAMP"		
	},
	{
		"name": "properties",
		"type": "SUPER"		
	}
];



module.exports = { eventsSchema, usersSchema, groupsSchema };