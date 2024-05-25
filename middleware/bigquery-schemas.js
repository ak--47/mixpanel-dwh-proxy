const eventsSchema = [
	{
		"name": "event",
		"type": "STRING",
		"mode": "REQUIRED",
		"description": "The name of the event"
	},
	{
		"name": "event_time",
		"type": "TIMESTAMP",
		"mode": "REQUIRED",
		"description": "The time the event occurred, UTC epoch"
	},
	{
		"name": "insert_time",
		"type": "TIMESTAMP",
		"mode": "REQUIRED",
		"description": "The time the event was ingested into the warehouse"
	},
	{
		"name": "token",
		"type": "STRING",
		"mode": "REQUIRED",
		"description": "The Mixpanel project token",
	},
	{
		"name": "device_id",
		"type": "STRING",
		"mode": "REQUIRED",
		"description": "The device ID or anonymous ID"
	},
	{
		"name": "insert_id",
		"type": "STRING",
		"mode": "REQUIRED",
		"description": "The Mixpanel insert ID or event ID"
	},
	{
		"name": "user_id",
		"type": "STRING",
		"mode": "NULLABLE",
		"description": "The Mixpanel user ID or canonical ID (sparse)"
	},
	{
		"name": "distinct_id",
		"type": "STRING",
		"mode": "NULLABLE",
		"description": "The Mixpanel distinct ID (legacy)"
	},
	{
		"name": "properties",
		"type": "JSON",
		"mode": "NULLABLE",
		"description": "The event's properties"
	}
];

const usersSchema = [
	{
		"name": "token",
		"type": "STRING",
		"mode": "REQUIRED",
		"description": "The Mixpanel project token"
	},
	{
		"name": "distinct_id",
		"type": "STRING",
		"mode": "REQUIRED",
		"description": "The Mixpanel distinct ID (user ID)"
	},
	{
		"name": "ip",
		"type": "STRING",
		"mode": "NULLABLE",
		"description": "The IP address of the user"
	},
	{
		"name": "insert_time",
		"type": "TIMESTAMP",
		"mode": "REQUIRED",
		"description": "The time the profile was ingested into the warehouse"
	},
	{
		"name": "operation",
		"type": "STRING",
		"mode": "REQUIRED",
		"description": "the type of profile operation: set, set_once, unset, delete"
	},
	{
		"name": "properties",
		"type": "JSON",
		"mode": "NULLABLE",
		"description": "The user's profile properties"
	}
];


const groupsSchema = [
	{
		"name": "token",
		"type": "STRING",
		"mode": "REQUIRED",
		"description": "The Mixpanel project token"
	},
	{
		"name": "group_key",
		"type": "STRING",
		"mode": "REQUIRED",
		"description": "The Mixpanel group key"
	},
	{
		"name": "group_id",
		"type": "STRING",
		"mode": "REQUIRED",
		"description": "The Mixpanel group ID"
	},
	{
		"name": "operation",
		"type": "STRING",
		"mode": "REQUIRED",
		"description": "The type of profile operation: set, set_once, unset, delete"
	},
	{
		"name": "insert_time",
		"type": "TIMESTAMP",
		"mode": "REQUIRED",
		"description": "The time the profile was ingested into the warehouse"
	},
	{
		"name": "properties",
		"type": "JSON",
		"mode": "NULLABLE",
		"description": "The group's properties"
	}
];



module.exports = { eventsSchema, usersSchema, groupsSchema };