namespace :serializr do
  task :thrift_gen => :environment do
    Serializr::Thrift::gen_idl
  end

  task :protobuf_gen => :environment do
    Serializr::ProtocolBuffers::gen_idl
  end
  
  task :avro_gen => :environment do
    Serializr::Avro::gen_idl
  end
end