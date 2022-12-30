import os
import sys
import json
import ydb

answer_obj = {
	"method": "sendMessage",
	"chat_id": 236079948,
	"text": "text_answer"
}


def send_answer():
	return {
		'ok': True,
		"statusCode": 200,
		"headers": {
			'Content-Type': 'application/json'
		},
		"body": json.dumps(answer_obj),
		"isBase64Encoded": False,
		"parse_mode": "MarkdownV2"
	}


driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
driver.wait(fail_fast=True, timeout=5)
session = driver.table_client.session().create()
db_string_path = os.getenv('YDB_DATABASE')
table_name = "scripts_statistics"


def select_simple(group_by):
	result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
		"""
		PRAGMA TablePathPrefix("{0}");
		SELECT {2}, SUM(run_count)
		FROM {1}
		GROUP BY {2};
		""".format(db_string_path, table_name, group_by),
		commit_tx=True
	)
	return str(result_sets[0].rows)


def handler(event, context):
	try:
		body = json.loads(event['body']);
		if 'message' in body and 'text' in body['message']:
			b_text = body['message']['text']
			db_query = ""
			if "@rvt_bot" in b_text:
					b_text = b_text.replace("@rvt_bot", "")
			if b_text == "/users":
					db_query = "user_name"
			elif b_text == "/scripts":
					db_query = "script_name"
			if db_query != "":
					answer_test = """runs count         {}\n""".format(db_query)
					test = select_simple(db_query)
					for row in json.loads(test.replace("'", '"')):
							answer_test += """
							{0}          {1}
							""".format(row["column1"], row[db_query])
					answer_obj['text'] = answer_test
			else:
					answer_obj['text'] = b_text
			chat_id = body['message']['chat']['id']
			answer_obj['chat_id'] = chat_id
			return send_answer()
		else:
			return send_answer()
	except Exception as e:
		tb2 = sys.exc_info()[2]
		line = tb2.tb_lineno
		full_ex = "Exception on line: {0}\nHas failure: {1}".format(str(line), str(e))
		answer_obj["text"] = full_ex
		return send_answer()
