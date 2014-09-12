module GoodData
  module Connectors
    module CSVParser

      class CSVParser < Base::BaseParser

        def initialize(metadata,options ={})
          @type = "csv_parser"
          super(metadata,options)
        end


        def define_mandatory_configuration
          super
        end

        def define_default_configuration
          {
              @type => {}
          }
        end

        def parse_entity(entity)
          begin
            source_csv = CSV.open(entity.runtime["source_filename"],:headers => true)
            source_csv_enumerator = source_csv.each
            output_filename = "#{@data_directory}#{entity.id}.csv"
            CSV.open(output_filename,"wb",:headers => true) do |csv|
              csv << header(entity)
              source_csv_enumerator.each do |row|
                parse_row(entity,row).each do |output_row|
                  csv << output_row
                end
              end
            end
            entity.runtime["parsed_filename"] = "#{@data_directory}#{entity.id}.csv"
          rescue => e
              pp e.message
              fail "ups"
          ensure
            source_csv.close
          end
        end


        private


        def parse_row(entity,source_row)
          rows = []
          custom_entity_settings = entity.custom
          if (!entity.dependent_on.nil?)

            type = ""

            if (custom_entity_settings.include?("type"))
              type = custom_entity_settings["type"]
            else
              type = "denormalized"
            end

            if (type == "normalized")
              row = CSV::Row.new(Base::Global::HISTORY_FILE_STRUCTURE,[],false)
              Base::Global::HISTORY_FILE_STRUCTURE.each do |field_name|
                if (custom_entity_settings.include?(field_name))
                  source_field = custom_entity_settings[field_name]
                  row[field_name] = source_row[source_field]
                else
                  raise Base::ParseException,"The history entity #{entity.id} (#{field_name}) don't have all necessery settings for normalisation"
                end
              end
              rows << row
            elsif (type == "denormalized")
              entity_field = entity.get_enabled_fields
              source_file_metadata_fields = []
              source_file_metadata_fields << custom_entity_settings["id"] if custom_entity_settings.include?("id")
              source_file_metadata_fields << custom_entity_settings["timestamp"] if custom_entity_settings.include?("timestamp")
              source_file_metadata_fields << custom_entity_settings["is_deleted"] if custom_entity_settings.include?("is_deleted")
              source_file_metadata_fields += custom_entity_settings["ignored"] if custom_entity_settings.include?("ignored")
              fields_to_normalize = entity_field - source_file_metadata_fields
              fields_to_normalize.each do |field_name|


                row = CSV::Row.new(Base::Global::HISTORY_FILE_STRUCTURE,[],false)
                Base::Global::HISTORY_FILE_STRUCTURE.each do |history_field_name|
                  if (history_field_name == "value")
                    row[history_field_name] = source_row[field_name]
                  elsif (history_field_name == "key")
                    row[history_field_name] = field_name
                  else
                    if (custom_entity_settings.include?(history_field_name))
                      source_field = custom_entity_settings[history_field_name]
                      row[history_field_name] = source_row[source_field]
                    else
                      raise Base::ParseException,"The history entity #{entity.id} don't have all necessery settings for normalisation, missing #{history_field_name}"
                    end
                  end

                end
                rows << row
              end
            else
              raise Base::ParseException,"Unknown history type for entity #{entity.id}. Type value is #{type} supported (normalized,denormalized)"
            end
          else
            row = CSV::Row.new(entity.get_enabled_fields,[],false)
            entity.get_enabled_fields.each do |field_name|
              row[field_name] = source_row[field_name]
            end
            rows << row
          end
          rows
        end

        def header(entity)
          custom_entity_settings = entity.custom
          if (custom_entity_settings.include?("dependent_on"))
            CSV::Row.new(Base::Global::HISTORY_FILE_STRUCTURE,Base::Global::HISTORY_FILE_STRUCTURE,true)
          else
            CSV::Row.new(entity.get_enabled_fields,entity.get_enabled_fields,true)
          end
        end






      end

    end
  end
end