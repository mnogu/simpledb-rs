use std::env;

use tonic::transport::Server;

use crate::{
    api::network::{
        remoteconnection::RemoteConnection,
        remotemetadata::RemoteMetaData,
        remoteresultset::RemoteResultSet,
        simpledb::{
            connection_server::ConnectionServer, meta_data_server::MetaDataServer,
            result_set_server::ResultSetServer, statement_server::StatementServer,
        },
    },
    server::simpledb::SimpleDB,
};

mod api;
mod buffer;
mod file;
mod index;
mod log;
mod metadata;
mod parse;
mod plan;
mod query;
mod record;
mod server;
mod tx;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let dirname;
    if args.len() == 1 {
        dirname = "studentdb";
    } else {
        dirname = &args[1];
    };
    let db = SimpleDB::new(dirname).unwrap();

    let addr = "[::1]:1099".parse().unwrap();

    let conn = RemoteConnection::new(db).unwrap();
    let stmt = conn.create_statement();
    let rs = RemoteResultSet::new(stmt.result_sets());
    let md = RemoteMetaData::new(stmt.result_sets());
    Server::builder()
        .add_service(ConnectionServer::new(conn))
        .add_service(StatementServer::new(stmt))
        .add_service(ResultSetServer::new(rs))
        .add_service(MetaDataServer::new(md))
        .serve(addr)
        .await?;

    println!("database server ready");
    Ok(())
}
