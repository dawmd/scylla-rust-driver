use crate::utils::{
    LwtRetryPolicy, PerformDDL as _, create_new_session_builder, disable_tablets_unless_supported,
    setup_tracing, unique_keyspace_name,
};
use assert_matches::assert_matches;
use scylla::client::session::Session;
use scylla::errors::{BadQuery, ExecutionError, RequestAttemptError};
use scylla::frame::frame_errors::{BatchSerializationError, CqlRequestSerializationError};
use scylla::response::query_result::{QueryResult, QueryRowsResult};
use scylla::statement::batch::{Batch, BatchStatement, BatchType};
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use scylla::value::Counter;
use std::collections::BTreeSet;
use std::sync::Arc;

#[tokio::test]
async fn batch_statements_and_values_mismatch_detected() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(&ks, false).await.unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS batch_serialization_test (p int PRIMARY KEY, val int)")
        .await
        .unwrap();

    let mut batch = Batch::new(BatchType::Logged);
    let stmt = session
        .prepare("INSERT INTO batch_serialization_test (p, val) VALUES (?, ?)")
        .await
        .unwrap();
    batch.append_statement(stmt.clone());
    batch.append_statement(Statement::new(
        "INSERT INTO batch_serialization_test (p, val) VALUES (3, 4)",
    ));
    batch.append_statement(stmt);

    // Subtest 1: counts are correct
    {
        session.batch(&batch, &((1, 2), (), (5, 6))).await.unwrap();
    }

    // Subtest 2: not enough values
    {
        let err = session.batch(&batch, &((1, 2), ())).await.unwrap_err();
        assert_matches!(
            err,
            ExecutionError::LastAttemptError(RequestAttemptError::CqlRequestSerialization(
                CqlRequestSerializationError::BatchSerialization(
                    BatchSerializationError::ValuesAndStatementsLengthMismatch {
                        n_value_lists: 2,
                        n_statements: 3
                    }
                )
            ))
        )
    }

    // Subtest 3: too many values
    {
        let err = session
            .batch(&batch, &((1, 2), (), (5, 6), (7, 8)))
            .await
            .unwrap_err();
        assert_matches!(
            err,
            ExecutionError::LastAttemptError(RequestAttemptError::CqlRequestSerialization(
                CqlRequestSerializationError::BatchSerialization(
                    BatchSerializationError::ValuesAndStatementsLengthMismatch {
                        n_value_lists: 4,
                        n_statements: 3
                    }
                )
            ))
        )
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
#[cfg_attr(cassandra_tests, ignore)]
async fn test_large_batch_statements() {
    setup_tracing();
    let mut session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session = create_test_session(session, &ks).await;

    let max_queries = u16::MAX as usize;
    let batch_insert_result = write_batch(&session, max_queries, &ks).await;

    batch_insert_result.unwrap();

    let too_many_queries = u16::MAX as usize + 1;
    let batch_insert_result = write_batch(&session, too_many_queries, &ks).await;
    assert_matches!(
        batch_insert_result.unwrap_err(),
        ExecutionError::BadQuery(BadQuery::TooManyQueriesInBatchStatement(_too_many_queries)) if _too_many_queries == too_many_queries
    );

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

async fn create_test_session(session: Session, ks: &str) -> Session {
    session
        .ddl(
            format!("CREATE KEYSPACE {ks} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }}"),
        )
        .await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE {ks}.pairs (dummy int, k blob, v blob, primary key (dummy, k))"
        ))
        .await
        .unwrap();
    session
}

async fn write_batch(session: &Session, n: usize, ks: &str) -> Result<QueryResult, ExecutionError> {
    let mut batch_query = Batch::new(BatchType::Unlogged);
    let mut batch_values = Vec::new();
    let statement_str = format!("INSERT INTO {ks}.pairs (dummy, k, v) VALUES (0, ?, ?)");
    let statement = Statement::new(statement_str);
    let prepared_statement = session.prepare(statement).await.unwrap();
    for i in 0..n {
        let mut key = vec![0];
        key.extend(i.to_be_bytes().as_slice());
        let value = key.clone();
        let values = vec![key, value];
        batch_values.push(values);
        batch_query.append_statement(prepared_statement.clone());
    }
    session.batch(&batch_query, batch_values).await
}

#[tokio::test]
async fn test_quietly_prepare_batch() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .ddl("CREATE TABLE test_batch_table (a int, b int, primary key (a, b))")
        .await
        .unwrap();

    let unprepared_insert_a_b: &str = "insert into test_batch_table (a, b) values (?, ?)";
    let unprepared_insert_a_7: &str = "insert into test_batch_table (a, b) values (?, 7)";
    let unprepared_insert_8_b: &str = "insert into test_batch_table (a, b) values (8, ?)";
    let unprepared_insert_1_2: &str = "insert into test_batch_table (a, b) values (1, 2)";
    let unprepared_insert_2_3: &str = "insert into test_batch_table (a, b) values (2, 3)";
    let unprepared_insert_3_4: &str = "insert into test_batch_table (a, b) values (3, 4)";
    let unprepared_insert_4_5: &str = "insert into test_batch_table (a, b) values (4, 5)";
    let prepared_insert_a_b: PreparedStatement =
        session.prepare(unprepared_insert_a_b).await.unwrap();
    let prepared_insert_a_7: PreparedStatement =
        session.prepare(unprepared_insert_a_7).await.unwrap();
    let prepared_insert_8_b: PreparedStatement =
        session.prepare(unprepared_insert_8_b).await.unwrap();

    {
        let mut fully_prepared_batch: Batch = Default::default();
        fully_prepared_batch.append_statement(prepared_insert_a_b);
        fully_prepared_batch.append_statement(prepared_insert_a_7.clone());
        fully_prepared_batch.append_statement(prepared_insert_8_b);

        session
            .batch(&fully_prepared_batch, ((50, 60), (50,), (60,)))
            .await
            .unwrap();

        assert_test_batch_table_rows_contain(&session, &[(50, 60), (50, 7), (8, 60)]).await;
    }

    {
        let mut unprepared_batch1: Batch = Default::default();
        unprepared_batch1.append_statement(unprepared_insert_1_2);
        unprepared_batch1.append_statement(unprepared_insert_2_3);
        unprepared_batch1.append_statement(unprepared_insert_3_4);

        session
            .batch(&unprepared_batch1, ((), (), ()))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(1, 2), (2, 3), (3, 4)]).await;
    }

    {
        let mut unprepared_batch2: Batch = Default::default();
        unprepared_batch2.append_statement(unprepared_insert_a_b);
        unprepared_batch2.append_statement(unprepared_insert_a_7);
        unprepared_batch2.append_statement(unprepared_insert_8_b);

        session
            .batch(&unprepared_batch2, ((12, 22), (12,), (22,)))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(12, 22), (12, 7), (8, 22)]).await;
    }

    {
        let mut partially_prepared_batch: Batch = Default::default();
        partially_prepared_batch.append_statement(unprepared_insert_a_b);
        partially_prepared_batch.append_statement(prepared_insert_a_7);
        partially_prepared_batch.append_statement(unprepared_insert_4_5);

        session
            .batch(&partially_prepared_batch, ((33, 43), (33,), ()))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(33, 43), (33, 7), (4, 5)]).await;
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

async fn assert_test_batch_table_rows_contain(sess: &Session, expected_rows: &[(i32, i32)]) {
    let selected_rows: BTreeSet<(i32, i32)> = sess
        .query_unpaged("SELECT a, b FROM test_batch_table", ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32)>()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    for expected_row in expected_rows.iter() {
        if !selected_rows.contains(expected_row) {
            panic!("Expected {selected_rows:?} to contain row: {expected_row:?}, but they didn't");
        }
    }
}

// Batches containing LWT queries (IF col = som) return rows with information whether the queries were applied.
#[tokio::test]
async fn test_batch_lwts() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    // This test uses LWT, which older ScyllaDB versions do not support on tablet keyspaces.
    let create_ks = format!(
        "CREATE KEYSPACE {ks} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}{}",
        disable_tablets_unless_supported(&session, "LWT_WITH_TABLETS").await
    );
    session.ddl(create_ks).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .ddl("CREATE TABLE tab (p1 int, c1 int, r1 int, r2 int, primary key (p1, c1))")
        .await
        .unwrap();

    session
        .query_unpaged("INSERT INTO tab (p1, c1, r1, r2) VALUES (0, 0, 0, 0)", ())
        .await
        .unwrap();

    let mut batch: Batch = Batch::default();
    // LWTs on a fresh table can time out while the server creates its Paxos
    // state table (see `LwtRetryPolicy`). Such an attempt is not applied, so
    // a retry still returns the expected rows.
    batch.set_retry_policy(Some(Arc::new(LwtRetryPolicy {
        retry_commit_timeouts: false,
    })));
    batch.append_statement("UPDATE tab SET r2 = 1 WHERE p1 = 0 AND c1 = 0 IF r1 = 0");
    batch.append_statement("INSERT INTO tab (p1, c1, r1, r2) VALUES (0, 123, 321, 312)");
    batch.append_statement("UPDATE tab SET r1 = 1 WHERE p1 = 0 AND c1 = 0 IF r2 = 0");

    let batch_res: QueryResult = session.batch(&batch, ((), (), ())).await.unwrap();
    let batch_deserializer = batch_res.into_rows_result().unwrap();

    // Scylla returns 5 columns, but Cassandra returns only 1
    let is_scylla: bool = batch_deserializer.column_specs().len() == 5;

    if is_scylla {
        test_batch_lwts_for_scylla(&session, &batch, &batch_deserializer).await;
    } else {
        test_batch_lwts_for_cassandra(&session, &batch, &batch_deserializer).await;
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

async fn test_batch_lwts_for_scylla(
    session: &Session,
    batch: &Batch,
    query_rows_result: &QueryRowsResult,
) {
    // Alias required by clippy
    type IntOrNull = Option<i32>;

    // Returned columns are:
    // [applied], p1, c1, r1, r2
    let batch_res_rows: Vec<(bool, IntOrNull, IntOrNull, IntOrNull, IntOrNull)> = query_rows_result
        .rows()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let expected_batch_res_rows = vec![
        (true, Some(0), Some(0), Some(0), Some(0)),
        (true, None, None, None, None),
        (true, Some(0), Some(0), Some(0), Some(0)),
    ];

    assert_eq!(batch_res_rows, expected_batch_res_rows);

    let prepared_batch: Batch = session.prepare_batch(batch).await.unwrap();
    let prepared_batch_res: QueryResult =
        session.batch(&prepared_batch, ((), (), ())).await.unwrap();

    let prepared_batch_res_rows: Vec<(bool, IntOrNull, IntOrNull, IntOrNull, IntOrNull)> =
        prepared_batch_res
            .into_rows_result()
            .unwrap()
            .rows()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

    let expected_prepared_batch_res_rows = vec![
        (false, Some(0), Some(0), Some(1), Some(1)),
        (false, None, None, None, None),
        (false, Some(0), Some(0), Some(1), Some(1)),
    ];

    assert_eq!(prepared_batch_res_rows, expected_prepared_batch_res_rows);
}

async fn test_batch_lwts_for_cassandra(
    session: &Session,
    batch: &Batch,
    query_rows_result: &QueryRowsResult,
) {
    // Alias required by clippy
    type IntOrNull = Option<i32>;

    // Returned columns are:
    // [applied]
    let batch_res_rows: Vec<(bool,)> = query_rows_result
        .rows()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    let expected_batch_res_rows = vec![(true,)];

    assert_eq!(batch_res_rows, expected_batch_res_rows);

    let prepared_batch: Batch = session.prepare_batch(batch).await.unwrap();
    let prepared_batch_res: QueryResult =
        session.batch(&prepared_batch, ((), (), ())).await.unwrap();

    // Returned columns are:
    // [applied], p1, c1, r1, r2
    let prepared_batch_res_rows: Vec<(bool, IntOrNull, IntOrNull, IntOrNull, IntOrNull)> =
        prepared_batch_res
            .into_rows_result()
            .unwrap()
            .rows()
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

    let expected_prepared_batch_res_rows = vec![(false, Some(0), Some(0), Some(1), Some(1))];

    assert_eq!(prepared_batch_res_rows, expected_prepared_batch_res_rows);
}

#[tokio::test]
async fn test_prepare_batch() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();

    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(ks.clone(), false).await.unwrap();

    session
        .ddl("CREATE TABLE test_batch_table (a int, b int, primary key (a, b))")
        .await
        .unwrap();

    let unprepared_insert_a_b: &str = "insert into test_batch_table (a, b) values (?, ?)";
    let unprepared_insert_a_7: &str = "insert into test_batch_table (a, b) values (?, 7)";
    let unprepared_insert_8_b: &str = "insert into test_batch_table (a, b) values (8, ?)";
    let prepared_insert_a_b: PreparedStatement =
        session.prepare(unprepared_insert_a_b).await.unwrap();
    let prepared_insert_a_7: PreparedStatement =
        session.prepare(unprepared_insert_a_7).await.unwrap();
    let prepared_insert_8_b: PreparedStatement =
        session.prepare(unprepared_insert_8_b).await.unwrap();

    let assert_batch_prepared = |b: &Batch| {
        for stmt in &b.statements {
            match stmt {
                BatchStatement::PreparedStatement(_) => {}
                _ => panic!("Unprepared statement in prepared batch!"),
            }
        }
    };

    {
        let mut unprepared_batch: Batch = Default::default();
        unprepared_batch.append_statement(unprepared_insert_a_b);
        unprepared_batch.append_statement(unprepared_insert_a_7);
        unprepared_batch.append_statement(unprepared_insert_8_b);

        let prepared_batch: Batch = session.prepare_batch(&unprepared_batch).await.unwrap();
        assert_batch_prepared(&prepared_batch);

        session
            .batch(&prepared_batch, ((10, 20), (10,), (20,)))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(10, 20), (10, 7), (8, 20)]).await;
    }

    {
        let mut partially_prepared_batch: Batch = Default::default();
        partially_prepared_batch.append_statement(unprepared_insert_a_b);
        partially_prepared_batch.append_statement(prepared_insert_a_7.clone());
        partially_prepared_batch.append_statement(unprepared_insert_8_b);

        let prepared_batch: Batch = session
            .prepare_batch(&partially_prepared_batch)
            .await
            .unwrap();
        assert_batch_prepared(&prepared_batch);

        session
            .batch(&prepared_batch, ((30, 40), (30,), (40,)))
            .await
            .unwrap();
        assert_test_batch_table_rows_contain(&session, &[(30, 40), (30, 7), (8, 40)]).await;
    }

    {
        let mut fully_prepared_batch: Batch = Default::default();
        fully_prepared_batch.append_statement(prepared_insert_a_b);
        fully_prepared_batch.append_statement(prepared_insert_a_7);
        fully_prepared_batch.append_statement(prepared_insert_8_b);

        let prepared_batch: Batch = session.prepare_batch(&fully_prepared_batch).await.unwrap();
        assert_batch_prepared(&prepared_batch);

        session
            .batch(&prepared_batch, ((50, 60), (50,), (60,)))
            .await
            .unwrap();

        assert_test_batch_table_rows_contain(&session, &[(50, 60), (50, 7), (8, 60)]).await;
    }

    {
        let mut bad_batch: Batch = Default::default();
        bad_batch.append_statement(unprepared_insert_a_b);
        bad_batch.append_statement("This isnt even CQL");
        bad_batch.append_statement(unprepared_insert_8_b);

        assert!(session.prepare_batch(&bad_batch).await.is_err());
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_batch() {
    setup_tracing();
    let session = Arc::new(create_new_session_builder().build().await.unwrap());
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t_batch (a int, b int, c text, primary key (a, b))"
        ))
        .await
        .unwrap();

    let prepared_statement = session
        .prepare(format!(
            "INSERT INTO {ks}.t_batch (a, b, c) VALUES (?, ?, ?)"
        ))
        .await
        .unwrap();

    // TODO: Add API that supports binding values to statements in batch creation process,
    // to avoid problem of statements/values count mismatch
    let mut batch: Batch = Default::default();
    batch.append_statement(&format!("INSERT INTO {ks}.t_batch (a, b, c) VALUES (?, ?, ?)")[..]);
    batch.append_statement(&format!("INSERT INTO {ks}.t_batch (a, b, c) VALUES (7, 11, '')")[..]);
    batch.append_statement(prepared_statement.clone());

    let four_value: i32 = 4;
    let hello_value: String = String::from("hello");
    let session_clone = session.clone();
    // We're spawning to a separate task here to test that it works even in that case, because in some scenarios
    // (specifically if the `BatchValuesIter` associated type is not dropped before await boundaries)
    // the implicit auto trait propagation on batch will be such that the returned future is not Send (depending on
    // some lifetime for some unknown reason), so can't be spawned on tokio.
    // See https://github.com/scylladb/scylla-rust-driver/issues/599 for more details
    tokio::spawn(async move {
        let values = (
            (1_i32, 2_i32, "abc"),
            (),
            (1_i32, &four_value, hello_value.as_str()),
        );
        session_clone.batch(&batch, values).await.unwrap();
    })
    .await
    .unwrap();

    let mut results: Vec<(i32, i32, String)> = session
        .query_unpaged(format!("SELECT a, b, c FROM {ks}.t_batch"), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, String)>()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    results.sort();
    assert_eq!(
        results,
        vec![
            (1, 2, String::from("abc")),
            (1, 4, String::from("hello")),
            (7, 11, String::from(""))
        ]
    );

    // Test repreparing statement inside a batch
    let mut batch: Batch = Default::default();
    batch.append_statement(prepared_statement);
    let values = ((4_i32, 20_i32, "foobar"),);

    // This statement flushes the prepared statement cache
    session
        .ddl(format!(
            "ALTER TABLE {ks}.t_batch WITH gc_grace_seconds = 42"
        ))
        .await
        .unwrap();
    session.batch(&batch, values).await.unwrap();

    let results: Vec<(i32, i32, String)> = session
        .query_unpaged(format!("SELECT a, b, c FROM {ks}.t_batch WHERE a = 4"), &[])
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i32, i32, String)>()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(results, vec![(4, 20, String::from("foobar"))]);

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

#[tokio::test]
async fn test_counter_batch() {
    setup_tracing();
    let session = Arc::new(create_new_session_builder().build().await.unwrap());
    let ks = unique_keyspace_name();

    // This test uses counters, which older ScyllaDB versions do not support
    // on tablet keyspaces.
    let create_ks = format!(
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}{}",
        disable_tablets_unless_supported(&session, "COUNTERS_WITH_TABLETS").await
    );
    session.ddl(create_ks).await.unwrap();
    session
        .ddl(format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t_batch (key int PRIMARY KEY, value counter)"
        ))
        .await
        .unwrap();

    let statement_str = format!("UPDATE {ks}.t_batch SET value = value + ? WHERE key = ?");
    let query = Statement::from(statement_str);
    let prepared = session.prepare(query.clone()).await.unwrap();

    let mut counter_batch = Batch::new(BatchType::Counter);
    counter_batch.append_statement(query.clone());
    counter_batch.append_statement(prepared.clone());
    counter_batch.append_statement(query.clone());
    counter_batch.append_statement(prepared.clone());
    counter_batch.append_statement(query.clone());
    counter_batch.append_statement(prepared.clone());

    // Check that we do not get a server error - the driver
    // should send a COUNTER batch instead of a LOGGED (default) one.
    session
        .batch(
            &counter_batch,
            (
                (Counter(1), 1),
                (Counter(2), 2),
                (Counter(3), 3),
                (Counter(4), 4),
                (Counter(5), 5),
                (Counter(6), 6),
            ),
        )
        .await
        .unwrap();

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

// This is a regression test for #1134.
#[tokio::test]
async fn test_batch_to_multiple_tables() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();

    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(&ks, true).await.unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS t_batch1 (a int, b int, c text, primary key (a, b))")
        .await
        .unwrap();
    session
        .ddl("CREATE TABLE IF NOT EXISTS t_batch2 (a int, b int, c text, primary key (a, b))")
        .await
        .unwrap();

    let prepared_statement = session
        .prepare(
            "
            BEGIN BATCH
                INSERT INTO t_batch1 (a, b, c) VALUES (?, ?, ?);
                INSERT INTO t_batch2 (a, b, c) VALUES (?, ?, ?);
            APPLY BATCH;
            ",
        )
        .await
        .unwrap();

    session
        .execute_unpaged(&prepared_statement, (1, 2, "ala", 4, 5, "ma"))
        .await
        .unwrap();

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}

// A batch can be prepared as a whole, as a single `BEGIN BATCH ... APPLY BATCH` statement.
// Then, bind indexes are global to the batch, and the server computes the indexes of the markers
// that bind the partition key by looking at all the statements of the batch.
// The token is known only if all statements target the same table and the partition key
// is fully bound by markers, regardless of which statements those markers come from.
#[tokio::test]
async fn test_prepared_batch_token() {
    setup_tracing();
    let session = create_new_session_builder().build().await.unwrap();
    let ks = unique_keyspace_name();
    session.ddl(format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}}")).await.unwrap();
    session.use_keyspace(&ks, false).await.unwrap();
    for table in ["batch_token_test1", "batch_token_test2"] {
        session
            .ddl(format!(
                "CREATE TABLE IF NOT EXISTS {table} (p int PRIMARY KEY, val int)"
            ))
            .await
            .unwrap();
    }
    session
        .ddl("CREATE TABLE IF NOT EXISTS batch_composite_pk_test (pk1 int, pk2 int, val int, PRIMARY KEY ((pk1, pk2)))")
        .await
        .unwrap();

    // Case 1: the partition key is bound by markers in a single table. The token is known.
    {
        let prepared = session
            .prepare(
                "BEGIN BATCH \
                 INSERT INTO batch_token_test1 (p, val) VALUES (?, ?); \
                 INSERT INTO batch_token_test1 (p, val) VALUES (30, ?); \
                 APPLY BATCH",
            )
            .await
            .unwrap();
        let values = (1, 2, 3);
        session.execute_unpaged(&prepared, values).await.unwrap();

        let single_stmt = session
            .prepare("INSERT INTO batch_token_test1 (p, val) VALUES (?, ?)")
            .await
            .unwrap();
        let expected = single_stmt.calculate_token(&(1, 0)).unwrap().unwrap();
        assert_eq!(prepared.calculate_token(&values).unwrap(), Some(expected));
    }

    // Case 2: the partition key is hardcoded, so it is not bound by a marker.
    // The token is unknown.
    {
        let prepared = session
            .prepare(
                "BEGIN BATCH \
                 INSERT INTO batch_token_test1 (p, val) VALUES (1, ?); \
                 APPLY BATCH",
            )
            .await
            .unwrap();
        let values = (2,);
        session.execute_unpaged(&prepared, values).await.unwrap();
        assert_eq!(prepared.calculate_token(&values).unwrap(), None);
    }

    // Case 3: the statements target different tables. The token is unknown,
    // even though each statement binds its partition key.
    {
        let prepared = session
            .prepare(
                "BEGIN BATCH \
                 INSERT INTO batch_token_test1 (p, val) VALUES (?, ?); \
                 INSERT INTO batch_token_test2 (p, val) VALUES (?, ?); \
                 APPLY BATCH",
            )
            .await
            .unwrap();
        let values = (1, 2, 3, 4);
        session.execute_unpaged(&prepared, values).await.unwrap();
        assert_eq!(prepared.calculate_token(&values).unwrap(), None);
    }

    // Used to compute the expected tokens.
    let single_stmt = session
        .prepare("INSERT INTO batch_composite_pk_test (pk1, pk2, val) VALUES (?, ?, ?)")
        .await
        .unwrap();

    // Case 1: no single statement binds the whole partition key, but together they do.
    // Bind indexes: 0 -> pk1, 1 -> val (1st statement); 2 -> pk2, 3 -> val (2nd statement).
    {
        let prepared = session
            .prepare(
                "BEGIN BATCH \
                 INSERT INTO batch_composite_pk_test (pk1, pk2, val) VALUES (?, 10, ?); \
                 INSERT INTO batch_composite_pk_test (pk1, pk2, val) VALUES (20, ?, ?); \
                 APPLY BATCH",
            )
            .await
            .unwrap();
        let values = (1, 2, 3, 4);
        session.execute_unpaged(&prepared, values).await.unwrap();

        // pk1 comes from bind index 0, pk2 from bind index 2.
        let expected = single_stmt.calculate_token(&(1, 3, 0)).unwrap().unwrap();
        assert_eq!(prepared.calculate_token(&values).unwrap(), Some(expected));
    }

    // Case 2: both statements bind pk1, so the last bind index wins for it.
    // Bind indexes: 0 -> pk1, 1 -> pk2, 2 -> val (1st statement); 3 -> val, 4 -> pk1 (2nd statement).
    // pk1 corresponds to the marker of the 2nd statement, pk2 to the marker of the 1st one.
    {
        let prepared = session
            .prepare(
                "BEGIN BATCH \
                 INSERT INTO batch_composite_pk_test (pk1, pk2, val) VALUES (?, ?, ?); \
                 UPDATE batch_composite_pk_test SET val = ? WHERE pk1 = ? AND pk2 = 10; \
                 APPLY BATCH",
            )
            .await
            .unwrap();
        let values = (1, 2, 3, 4, 5);
        session.execute_unpaged(&prepared, values).await.unwrap();

        // pk1 comes from bind index 4, pk2 from bind index 1.
        let expected = single_stmt.calculate_token(&(5, 2, 0)).unwrap().unwrap();
        let unexpected1 = single_stmt.calculate_token(&(1, 2, 0)).unwrap().unwrap();
        let unexpected2 = single_stmt.calculate_token(&(5, 10, 0)).unwrap().unwrap();
        assert_eq!(prepared.calculate_token(&values).unwrap(), Some(expected));
        assert_ne!(
            prepared.calculate_token(&values).unwrap(),
            Some(unexpected1)
        );
        assert_ne!(
            prepared.calculate_token(&values).unwrap(),
            Some(unexpected2)
        );
    }

    session.ddl(format!("DROP KEYSPACE {ks}")).await.unwrap();
}
