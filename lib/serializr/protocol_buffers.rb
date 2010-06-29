module Serializr
  module ProtocolBuffers
    extend ActiveSupport::Concern
    
    def self.require_generated_source
      if File.exists?("#{Rails.root}/lib/serializr/protobuf")
        require "#{Rails.root}/lib/serializr/protobuf/serializr_app.pb.rb"
      end
    end
    
    def self.gen_idl
      FileUtils.mkdir_p  "#{Rails.root}/lib/serializr/protobuf"
      schema_file = open "#{Rails.root}/lib/serializr/protobuf/serializr_app.proto", "w"
      schema_file.write "package SerializrProtobuf;"
      self.serialized_models.map do |klass|
        schema_file.write "\n\n#{klass.to_protobuf_message}"
      end
      schema_file.close
      system "cd #{Rails.root}/lib/serializr/protobuf; rprotoc serializr_app.proto"
    end
    
    def self.serialized_models
      Dir.glob( Rails.root + 'app/models/*' ).map do |f|
        klass = File.basename( f ).gsub( /^(.+).rb/, '\1').camelize.constantize
        klass if klass.respond_to? :to_protobuf_message
      end.compact
    end
        
    module ClassMethods
      def to_protobuf_message
        num = 0
        types_conversion = {
          :primary_key => "int32",
          :float => "float",
          :decinal => "float",
          :string => "string",
          :integer => "int32",
          :boolean => "bool",
          :text => "string",
          :time => "int32",
          :datetime => "int32",
          :date => "int32",
          :timestamp => "int32",
          :binary => "bytes"
        }
        attrs = columns_hash.map do |k,v|
          num += 1
          "required #{types_conversion[v.type] || "string"} #{k} = #{num};"
        end
        "message #{self.name} { \n\t#{attrs.join("\n\t")} \n}"
      end
      
      def attrs_from_protobuf rec
        rec.fields.values.inject({}) do |h,f|
          col = self.columns_hash[f.name.to_s]
          if col && [:date, :datetime, :time, :timestamp].include?(col.type)
            val = Time.at(rec.send(f.name)) rescue nil
          else
            val = rec.send(f.name)
          end
          h.merge(f.name => val)
        end
      end
      
      def from_protobuf rec
        attrs = attrs_from_protobuf(rec)
        attrs.delete "id"
        self.new attrs
      end
      
    end
    
    module InstanceMethods
      def to_protobuf(serialize_it=false)
        h = self.class.columns.inject({}) do |hh,col|
          if [:time, :datetime, :timestamp, :date].include? col.type
            v = self.send(col.name).to_time.to_i
          else
            v = self.send(col.name)
          end
          hh.merge(col.name => v)
        end
        pb = "SerializrProtobuf::#{self.class.name}".constantize.new(h)
        serialize_it ? pb.serialize_to_string : pb
      end
    end
  end
end