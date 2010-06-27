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
      FileUtils.mkdir_p  "#{Rails.root}/lib/serializr/"
      schema_file = open "#{Rails.root}/lib/serializr/serializr_app.thrift", "w"
      schema_file.write "namespace rb SerializrModel\n\n"
      schema_file.write "exception NotFoundError { \n\t1: string message \n}\n"
      schema_file.write "exception ValidationError { \n\t1: list<string> errors \n}\n"
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
    
    def self.rack_middleware hook_path="/thrift", protocol_factory=::Thrift::BinaryProtocolAcceleratedFactory
      [
        Serializr::Thrift::RackMiddleware, 
        {
          :processor => SerializrModel::SerializrApp::Processor.new(self.handler.new), 
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
          :text => "string",
          :time => "i32",
          :datetime => "i32"
        }
        attrs = columns_hash.map do |k,v|
          num += 1
          "#{num}: #{types_conversion[v.type] || "string"} #{k}"
        end
        "struct #{self.name} { \n\t#{attrs.join("\n\t")} \n}"
      end
      
      def to_thrift_service
        services = ["", "# #{self.name} Service"]
        services << "list<#{self.name}> list#{self.name.pluralize}(1: i32 limit)"
        services << "i32 count#{self.name.pluralize}()"
        services << "#{self.name} get#{self.name}(1: i32 id) throws (1: NotFoundError err)"
        services << "list<#{self.name}> get#{self.name.pluralize}(1: list<i32> ids)"
        services << "i32 delete#{self.name}(1: i32 id) throws (1: NotFoundError err)"
        services << "i32 create#{self.name}(1: #{self.name} #{self.name.downcase}) throws (1: ValidationError err)"
        services << "#{self.name} update#{self.name}(1: #{self.name} #{self.name.downcase})  throws (1: NotFoundError not_found_error, 2: ValidationError validation_error)"
        services
      end
      
      def thrift_handler_module
        klass_name = self.name
        klass = self
        Module.new do |m|
          
          # countRecords
          m.send(:define_method, "count#{klass_name.pluralize}".to_sym) do
            klass.count
          end
          
          # getRecords
          m.send(:define_method, "get#{klass_name.pluralize}".to_sym) do |ids|
            klass.all(:conditions => { :id => ids }).map(&:to_thrift)
          end
          
          # listRecords
          m.send(:define_method, "list#{klass_name.pluralize}".to_sym) do |limit|
            klass.all(:limit => limit).map(&:to_thrift)
          end
          
          # getRecord
          m.send(:define_method, "get#{klass_name}".to_sym) do |id|
            begin
              klass.find(id).to_thrift
            rescue => e
              raise SerializrModel::NotFoundError.new "#{klass.name} ##{id} does not exist"
            end
          end
          
          # deleteRecord
          m.send(:define_method, "delete#{klass_name}".to_sym) do |id|
            begin
              id if klass.destroy(id)
            rescue => e
              raise SerializrModel::NotFoundError.new "#{klass.name} ##{id} does not exist"
            end
          end
          
          # createRecord
          m.send(:define_method, "create#{klass_name}".to_sym) do |thrift_record|
            record = klass.from_thrift(thrift_record)
            begin
              record.save!
              record.id
            rescue => e
              err = SerializrModel::ValidationError.new
              err.errors = record.errors.full_messages
              raise err
            end
          end
          
          # updateRecord
          m.send(:define_method, "update#{klass_name}".to_sym) do |thrift_record|
            attrs = klass.attrs_from_thrift(thrift_record)
            record_id = attrs.delete("id")
            record = klass.find_by_id(record_id)
            record.to_thrift if record.update_attributes!(attrs)
          end
        end
      end
      
      def attrs_from_thrift rec
        rec.struct_fields.values.inject({}) do |h,f|
          h.merge(f[:name] => rec.send(f[:name]))
        end
      end
      
      def from_thrift rec
        attrs = attrs_from_thrift(rec)
        attrs.delete "id"
        self.new(attrs)
      end
    end
    
    module InstanceMethods
      def to_thrift
        h = self.serializable_hash.inject({}) do |hh,f|
          v = (f[1].is_a?(Date) || f[1].is_a?(Time)) ? f[1].to_time.to_i : f[1]
          hh.merge(f[0] => v)
        end
        "SerializrModel::#{self.class.name}".constantize.new(h)
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