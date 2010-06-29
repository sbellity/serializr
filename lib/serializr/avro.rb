module Serializr
  module Avro
    extend ActiveSupport::Concern
    
    def self.gen_idl
      FileUtils.mkdir_p  "#{Rails.root}/lib/serializr/avro"
      schema_file = open "#{Rails.root}/lib/serializr/avro/serializr_app.avro", "w"
      self.serialized_models.map do |klass|
        schema_file.write "\n\n#{Yajl::Encoder.encode(klass.to_avro_protocol, :pretty => true)}"
      end
      schema_file.close
    end
    
    def self.serialized_models
      Dir.glob( Rails.root + 'app/models/*' ).map do |f|
        klass = File.basename( f ).gsub( /^(.+).rb/, '\1').camelize.constantize
        klass if klass.respond_to? :to_avro_schema
      end.compact
    end
    
    
    module ClassMethods
      def to_avro_schema
        types_conversion =                                                                     {
          :primary_key => "int",
          :float => "float",
          :decinal => "double",
          :string => "string",
          :integer => "int",
          :boolean => "boolean",
          :text => "string",
          :time => "int",
          :datetime => "int",
          :date => "int",
          :timestamp => "int",
          :binary => "bytes"
        }
        
        {
          "type" => "record",
          "name" => self.name,
          "fields" => self.columns_hash.map { |name,col| { 
            "name" => name,
            "type" => (types_conversion[col.type] || "string")
          }}
        }
      end
      
      
      
      def to_avro_protocol
         {
           "namespace" => "#{self.name.pluralize.downcase}.proto",
           "protocal" => self.name.pluralize,
           "types" => [ self.to_avro_schema ],
           "messages" => {
             "list" => {
               "request" => [{ "name" => "limit", "type" => "int" }],
               "response" => { "type" => "array", "items" => self.name }
             },
             "get" => {
               "request" => [{ "name" => "id", "type" => "int" }],
               "response" => self.name
             },
             "delete" => {
               "request" => [{ "name" => "id", "type" => "int" }],
               "response" => { "name" => "id", "type" => "int" }
             },
           }
         }
      end
      
      def avro_responder_module
        
      end
    end
    
    module InstanceMethods
      
    end
    
  end
end