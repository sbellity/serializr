module Serializr
  module Thrift
    extend ActiveSupport::Concern
    
    def self.require_generated_source
      if File.exists?("#{Rails.root}/lib/serializr/gen-rb")
        $: << "#{Rails.root}/lib/serializr/gen-rb"
        require "#{Rails.root}/lib/serializr/gen-rb/serializr_app"
      end
    end
    
    def self.gen_idl
      FileUtils.mkdir_p Rails.root + "lib/serializr/"
      schema_file = open(Rails.root + "lib/serializr/serializr_app.thrift", "w")
      schema_file.write("namespace rb SerializrModel\n")
      services = []
      self.serialized_models.map do |klass|
        schema_file.write "\n\n#{klass.to_thrift_struct}"
        services << klass.to_thrift_service
      end
      schema_file.write "\n\nservice SerializrApp { \n\t#{services.flatten.join("\n\t")} \n}"
      schema_file.close
      system "cd #{Rails.root}/lib/serializr; thrift --gen rb serializr_app.thrift"
    end
    
    def self.serialized_models
      Dir.glob( Rails.root + 'app/models/*' ).map do |f|
        klass = File.basename( f ).gsub( /^(.+).rb/, '\1').camelize.constantize
        klass if klass.respond_to? :to_thrift_service
      end
    end
    
    def self.handler
      sm = self.serialized_models
      Class.new do |k|
        k.class_eval do 
          sm.map { |m| include m.thrift_handler_module }
        end
      end
    end
    
    def self.processor
      SerializrModel::SerializrApp::Processor.new(self.handler)
    end
    
    def self.rack_middleware hook_path="/thrift", protocol_factory=::Thrift::BinaryProtocolAcceleratedFactory
      [
        Serializr::Thrift::RackMiddleware, 
        {
          :processor => self.processor, 
          :hook_path => hook_path, 
          :protocol_factory => protocol_factory.new 
        }
      ]
    end
    
    module ClassMethods
      def to_thrift_struct
        num = 0
        types_conversion = {
          :string => "string",
          :integer => "i32",
          :date => "i32",
          :boolean => "bool",
          :text => "string"
        }
        attrs = columns_hash.map do |k,v|
          num += 1
          "#{num}: #{types_conversion[v.type] || "string"} #{k}"
        end
        "struct #{self.name} { \n\t#{attrs.join("\n\t")} \n}"
      end
      
      def to_thrift_service
        services = []
        services << "list<#{self.name}> list#{self.name.pluralize}(1: i32 limit)"
        services << "#{self.name} get#{self.name}(1: i32 id)"
        services << "#{self.name} delete#{self.name}(1: i32 id)"
        services
      end
      
      def thrift_handler_module
        klass_name = self.name
        klass = self
        Module.new do |m|
          m.send(:define_method, "get#{klass_name}".to_sym) do |id|
            klass.find_by_id(id).to_thrift
          end
          m.send(:define_method, "list#{klass_name.pluralize}".to_sym) do |limit|
            klass.all(:limit => limit).map(&:to_thrift)
          end
          m.send(:define_method, "delete#{klass_name}".to_sym) do |id|
            klass.destroy(id)
          end
        end
      end
      
    end
    
    module InstanceMethods
      def to_thrift
        "SerializrModel::#{self.class.name}".constantize.new(self.serializable_hash)
      end
    end
    
    class RackMiddleware
      attr_reader :hook_path, :processor, :protocol_factory

      def initialize(app, options = {})
        @app              = app
        @processor        = options[:processor] || (raise ArgumentError, "You have to specify a processor.")
        @protocol_factory = options[:protocol_factory] || BinaryProtocolFactory.new
        @hook_path        = options[:hook_path] || "/thrift"
      end

      def call(env)
        request = Rack::Request.new(env)
        if request.path == hook_path
          output = StringIO.new
          transport = ::Thrift::IOStreamTransport.new(request.body, output)
          protocol = @protocol_factory.get_protocol(transport)
          @processor.process(protocol, protocol)

          output.rewind
          response = Rack::Response.new(output)
          response["Content-Type"] = "application/x-thrift"
          response.finish
        else
          @app.call(env)
        end
      end
    end
  end
end