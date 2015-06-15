require "aws-sdk-v1"
require "forwardable"
require "blobby/key_constraint"

AWS.eager_autoload!(AWS::Core)
AWS.eager_autoload!(AWS::S3)

module Blobby

  # A BLOB store backed by an S3 bucket.
  #
  class S3Store

    # Create a new instance.
    #
    # If more than one bucket_name is specified, the store will write synchronously
    # to the first bucket, and asynchronously to the rest.
    #
    # bucket_names - name of bucket(s) to store things in
    # object_acl   - a canned access control policy
    # s3_options   - options passed to AWS::S3.new
    #
    def initialize(bucket_names, object_acl = :private, logger = nil, s3_options = {})
      @bucket_names = Array(bucket_names).map(&:to_str)
      @s3_options = s3_options.dup
      @s3_options.freeze
      @logger = logger || Logger.new("/dev/null")
      @object_acl = object_acl || :private  # Reset to AWS default if explicitly set to nil
    end

    attr_reader :bucket_names
    attr_reader :s3_options

    def bucket_name
      bucket_names.first
    end

    def available?
      buckets.first.objects.first
      true
    rescue ::AWS::Errors::Base
      false
    end

    def [](key)
      KeyConstraint.must_allow!(key)
      copies = buckets.map { |bucket| bucket.objects[key] }
      StoredObject.new(copies, object_acl, logger)
    end

    class StoredObject

      def initialize(copies, acl, logger = nil)
        @copies = copies
        @logger = logger
        @acl = acl
      end

      def exists?
        primary_copy.exists?
      end

      def read(&block)
        return nil unless primary_copy.exists?
        if block_given?
          primary_copy.read(&block)
          nil
        else
          primary_copy.read
        end
      end

      def write(payload)
        write_primary(payload)
        mirror_in_background
        nil
      end

      def delete
        primary_result = delete_primary
        mirror_deletes_in_background
        primary_result
      end

      private

      def write_primary(payload)
        with_logging("wrote", "failed to write", object_url(primary_copy)) do
          primary_copy.write(payload, :acl => acl)
        end
      end

      def delete_primary
        delete_object(primary_copy)
      end

      def with_logging(did, did_not, thing)
        start = Time.now
        yield
        logger.info { "#{did} #{thing} in #{Time.now - start}s" }
      rescue => e
        logger.error { "#{did_not} #{thing}" }
        logger.error { exception_message(e) }
        raise e
      end

      def exception_message(e)
        lines = [e.message]
        e.backtrace.each do |backtrace_line|
          lines << ("  " + backtrace_line)
        end
        lines.join("\n")
      end

      def mirror_in_background
        secondary_copies.each do |copy|
          Thread.new do
            with_logging("copied", "failed to copy", "#{object_url(primary_copy)} to #{object_url(copy)}") do
              copy.copy_from(primary_copy, :acl => acl)
            end
          end
        end
      end

      def mirror_deletes_in_background
        secondary_copies.each do |copy|
          Thread.new { delete_object(copy) }
        end
      end

      def delete_object(object)
        return false unless object.exists?
        with_logging("deleted", "failed to delete", object_url(object)) do
          object.delete
        end
        true
      end

      def object_url(o)
        "s3://#{o.bucket.name}/#{o.key}"
      end

      def primary_copy
        @copies.first
      end

      def secondary_copies
        @copies.drop(1)
      end

      attr_reader :logger
      attr_reader :acl

    end

    private

    def s3_client
      defaults = {
        :s3_endpoint => "s3.amazonaws.com"
      }
      ::AWS::S3.new(defaults.merge(s3_options))
    end

    def buckets
      bucket_names.map do |name|
        s3_client.buckets[name]
      end
    end

    attr_reader :logger
    attr_reader :object_acl

  end

end
