# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|

  gem.authors       = ["Mike Williams"]
  gem.email         = ["mdub@dogbiscuit.org"]
  gem.summary       = "Store BLOBs in S3"
  gem.homepage      = "https://github.com/realestate-com.au/blobby-s3"

  gem.name          = "blobby-s3"
  gem.version       = "1.0.0-alpha"

  gem.files         = `git ls-files`.split($OUTPUT_RECORD_SEPARATOR)
  gem.executables   = gem.files.grep(%r{^bin/}).map { |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_runtime_dependency("aws-sdk-v1", "~> 1.61.0")
  gem.add_runtime_dependency("blobby")

end
