pub mod networkconnection;
pub mod networkdriver;
pub mod networkmetadata;
pub mod networkresultset;
pub mod networkstatement;
pub mod simpledb {
    tonic::include_proto!("simpledb");
}
pub mod remoteconnection;
pub mod remotemetadata;
pub mod remoteresultset;
pub mod remotestatement;
