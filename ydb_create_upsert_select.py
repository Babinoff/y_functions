import os
import ydb
import sys
import json
import uuid

# Create driver in global space.
driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
session = driver.table_client.session().create()
db_string_path = os.getenv('YDB_DATABASE')
table_name = "scripts_statistics"


def create_tables():
	session.create_table(
		os.path.join(db_string_path, table_name),
		ydb.TableDescription()
		.with_column(ydb.Column('uuid', ydb.PrimitiveType.Utf8))  # not null column 
		.with_column(ydb.Column('script_name', ydb.OptionalType(ydb.PrimitiveType.Utf8)))
		.with_column(ydb.Column('user_name', ydb.OptionalType(ydb.PrimitiveType.Utf8)))
		.with_column(ydb.Column('run_count', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
		.with_column(ydb.Column('revit_version', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
		.with_column(ydb.Column('dynamo_version', ydb.OptionalType(ydb.PrimitiveType.Utf8)))
		.with_primary_key('uuid')
	)


def upsert_simple(script_name, user_name, run_count, revit_version, dynamo_version):
	if run_count == 1:
		session.transaction().execute(
			"""
			PRAGMA TablePathPrefix("{0}");
			UPSERT INTO {1} (script_name, user_name, run_count, revit_version, dynamo_version, uuid) VALUES ("{2}", "{3}", {4}, {5}, "{6}", "{7}");
			""".format(db_string_path, table_name, script_name, user_name, run_count, revit_version, dynamo_version, str(uuid.uuid4())),
			commit_tx=True
		)
	else:
		session.transaction().execute(
			"""
			PRAGMA TablePathPrefix("{0}");
			UPDATE {1}
			SET run_count = {2}
			WHERE script_name = '{3}' AND user_name = '{4}' AND revit_version = {5} AND dynamo_version = '{6}';
			""".format(db_string_path, table_name, run_count, script_name, user_name, revit_version, dynamo_version),
			commit_tx=True
		)


def select_simple(script_name, user_name, revit_version, dynamo_version):
	result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
		"""
		PRAGMA TablePathPrefix("{0}");
		SELECT script_name, user_name, run_count
		FROM {1} 
		WHERE script_name = '{2}' AND user_name = '{3}' AND revit_version = {4} AND dynamo_version = '{5}';
		""".format(db_string_path, table_name, script_name, user_name, revit_version, dynamo_version),
		commit_tx=True
	)
	return result_sets


def handler(event, context):
	try:
		create_tables()
		if "queryStringParameters" in event:
			sn = event["queryStringParameters"]["script_name"]
			un = event["queryStringParameters"]["user_name"]
			rv = int(event["queryStringParameters"]["revit_version"])
			dv = event["queryStringParameters"]["dynamo_version"]
		elif "debug-test" in event:  
			sn = event["script_name"]
			un = event["user_name"]
			rv = int(event["revit_version"])
			dv = event["dynamo_version"]
		result_sets = select_simple(sn, un, rv, dv)
		if len(result_sets[0].rows) == 1:
			upsert_simple(sn, un, result_sets[0].rows[0]["run_count"]+1, rv, dv)
		else:
			upsert_simple(sn, un, 1, rv, dv)
		result_sets_after = select_simple(sn, un, rv, dv)  
		return {
			'statusCode': 200,
			'body': str(result_sets_after[0].rows)
		}
	except Exception as e:
		tb2 = sys.exc_info()[2]
		line = tb2.tb_lineno
		if "queryStringParameters" in event:
			full_ex = "Exception on line: {0}\n Has failure: {1}\n event queryStringParameters: {2} ".format(str(line), str(e), str(event["queryStringParameters"]))
		else:
			full_ex = "Exception on line: {0}\n Has failure: {1}\n event: {2} ".format(str(line), str(e), str(event))
		return {
			'statusCode': 400,
			'body': full_ex,
		}
