require "gooddata_connectors_csv_parser/version"
require "gooddata_connectors_csv_parser/csv_parser"


module GoodData
  module Connectors
    module CSVParser
      class CSVParserMiddleware < GoodData::Bricks::Middleware

        def call(params)
          $log = params["GDC_LOGGER"]
          $log.info "Initializing CSV Parser"
          csv_parser = CSVParser.new(params["metadata_wrapper"],params)
          @app.call(params.merge('csv_parser_wrapper' => csv_parser))
        end
      end

    end
  end
end
