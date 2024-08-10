{'conf': <airflow.configuration.AirflowConfigParser object at 0x7fe4e4514650>, 
 'dag': <DAG: s3_to_emr_spark_with_xcoms_pro_versionsssssssss>,
   'dag_run': <DagRun s3_to_emr_spark_with_xcoms_pro_versionsssssssss @ 2024-08-09 00:00:00+00:00: scheduled__2024-08-09T00:00:00+00:00, 
   state:running, queued_at: 2024-08-10 13:44:05.527755+00:00. externally triggered: False>, 
   'data_interval_end': DateTime(2024, 8, 10, 0, 0, 0, tzinfo=Timezone('UTC')), 
   'data_interval_start': DateTime(2024, 8, 9, 0, 0, 0, tzinfo=Timezone('UTC')), 
   'ds': '2024-08-09', 'ds_nodash': '20240809', 
'expanded_ti_count': None, 'inlets': [], 
 'logical_date': DateTime(2024, 8, 9, 0, 0, 0, tzinfo=Timezone('UTC')), 
 'macros': <module 'airflow.macros' from '/usr/local/airflow/.local/lib/python3.11/site-packages/airflow/macros/__init__.py'>, 'map_index_template': None, 'next_ds': <Proxy at 0x7fe4de57b5c0
 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7fe4dcff1da0>, 'next_ds', '2024-08-10')>, 
     'next_ds_nodash': <Proxy at 0x7fe4dd0507c0 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7fe4dcff1da0>,
 'next_ds_nodash', '20240810')>, 
'next_execution_date': <Proxy at 0x7fe4dd053fc0 with factory functools.partial
(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7fe4dcff1da0>, 
'next_execution_date', DateTime(2024, 8, 10, 0, 0, 0, tzinfo=Timezone('UTC')))>, 
'outlets': [], 'params': {}, 'prev_data_interval_start_success': None, 
'prev_data_interval_end_success': None, 'prev_ds': <Proxy at 0x7fe4dd038940
 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory
at 0x7fe4dcff1da0>, 'prev_ds', '2024-08-08')>, 
'prev_ds_nodash': <Proxy at 0x7fe4dd06bb00 with factory functools.partia
l(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory 
 at 0x7fe4dcff1da0>, 'prev_ds_nodash', '20240808')>, 
 'prev_execution_date': <Proxy at 0x7fe4dd06bbc0 with factory fu
nctools.partial(<function lazy_mapping_from_context.<locals>._deprecated_
proxy_factory at 0x7fe4dcff1da0>,
'prev_execution_date', DateTime(2024, 8, 8, 0, 0, 0, tzinfo=Timezone('UTC')))>,
'prev_execution_date_success': <Proxy at 0x7fe4dd06bc80 with factory functools.partia
l(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7fe4dcff1da0>,
'prev_execution_date_success', None)>, 'prev_start_date_success': None, 
'prev_end_date_success': None, 'run_id': 'scheduled__2024-08-09T00:00:00+00:00',
'task': <Task(PythonOperator): push_s3_key>,
'task_instance': <TaskInstance: s3_to_emr_spark_with_xcoms_pro_versionsssssssss.push_s3_key scheduled__
2024-08-09T00:00:00+00:00 [running]>,
'task_instance_key_str': 's3_to_emr_spark_with_xcoms_pro_versionsssssssss__push_s3_key__20240809',
'test_mode': False, 'ti': <TaskInstance: s3_to_emr_spark_with_xcoms_pro_versionsssssssss.push_s3_key 
scheduled__2024-08-09T00:00:00+00:00 [running]>, 
'tomorrow_ds': <Proxy at 0x7fe4dd06bd40 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7fe4dcff1da0>, 
'tomorrow_ds', '2024-08-10')>, 
'tomorrow_ds_nodash': <Proxy at 0x7fe4dd06be00 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7fe4dcff1da0>,
 'tomorrow_ds_nodash', '20240810')>, \
    'triggering_dataset_events': <Proxy at 0x7fe4dd0024c0 with factory <function _get_template_context.<locals>.get_triggering_events at 0x7fe4dcff1760>>, 
    'ts': '2024-08-09T00:00:00+00:00', 'ts_nodash': '20240809T000000', 'ts_nodash_with_tz': '20240809T000000+0000',
      'var': {'json': None, 'value': None}, 'conn': None, 
      'yesterday_ds': <Proxy at 0x7fe4dd06bec0 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7fe4dcff1da0>,
'yesterday_ds', '2024-08-08')>, 'yesterday_ds_nodash':
<Proxy at 0x7fe4dd06bf80 with factory functools.partial(<function lazy_mapping_from_context.<locals>._deprecated_proxy_factory at 0x7fe4dcff1da0>, 'yesterday_ds_nodash', '20240808')>, 
'templates_dict': None}
