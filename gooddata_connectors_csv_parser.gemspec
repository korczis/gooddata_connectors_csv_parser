# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'gooddata_connectors_csv_parser/version'

Gem::Specification.new do |spec|
  spec.name          = "gooddata_connectors_csv_parser"
  spec.version       = GoodData::Connectors::CSVParser::VERSION
  spec.authors       = ["Adrian Toman"]
  spec.email         = ["adrian.toman@gooddata.com"]
  spec.description   = %q{The CSV Parser for gooddata connectors infrastructure}
  spec.summary       = %q{The CSV Parser for gooddata connectors infrastructure}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
end
