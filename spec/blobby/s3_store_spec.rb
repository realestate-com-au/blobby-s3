require "aws-sdk-v1"
require "blobby/s3_store"
require "fake_aws/s3"
require "logger"
require "stringio"

# Load the abstract "Store" tests from "blobby".
# This depends on the gem being packaged with "spec" dir intact.
$LOAD_PATH << Gem.loaded_specs["blobby"].full_gem_path + "/spec"

require "blobby/store_behaviour"

describe Blobby::S3Store do

  let(:fake_s3) { FakeAWS::S3.new }

  before do
    allow(::AWS::S3).to receive(:new).and_return(fake_s3)
  end

  let(:bucket_name) { "test-bucket" }
  let(:bucket) { fake_s3.buckets[bucket_name] }

  subject do
    described_class.new(bucket_name)
  end

  it_behaves_like Blobby::Store

  describe "#write" do

    let(:key) { "data/file" }
    let(:content) { "CONTENT" }

    before do
      subject[key].write(content)
    end

    it "stores stuff in S3" do
      expect(bucket.objects[key].read).to eq(content)
    end

  end

  describe "#delete" do
    let(:key) { "my_key" }

    before do
      subject[key].write("content")
      subject[key].delete
    end

    it "removes stuff from S3" do
      expect(bucket.objects[key]).to_not exist
    end
  end

  context "when we can't talk to S3" do

    before do
      allow(bucket.objects).to receive(:first) do
        fail ::AWS::S3::Errors::InvalidAccessKeyId, "urk!"
      end
    end

    it { is_expected.not_to be_available }

  end

  context "when the bucket does not exist" do

    before do
      allow(bucket.objects).to receive(:first) do
        fail ::AWS::S3::Errors::NoSuchBucket, "urk!"
      end
    end

    it { is_expected.not_to be_available }

  end

  let(:log_buffer) { StringIO.new }
  let(:logger) { Logger.new(log_buffer) }
  let(:log_output) { log_buffer.string }

  context "with a Logger" do
    let(:key) { "foo" }

    subject do
      described_class.new(bucket_name, {}, logger)
    end

    context "when successful" do
      before do
        subject[key].write("bar")
      end

      describe "#write" do
        it "logs the write" do
          expect(log_output).to include(%(wrote s3://test-bucket/foo))
        end
      end

      describe "#delete" do
        it "logs the delete" do
          subject[key].delete
          expect(log_output).to include(%(deleted s3://test-bucket/foo))
        end
      end
    end

    context "when unsuccessful" do
      describe "#write" do
        let(:do_write) { subject[key].write("bar") }

        before do
          allow(bucket.objects[key]).to receive(:write) { fail "something bad happened" }
        end

        it "logs the error" do
          do_write rescue nil
          expect(log_output).to include(%(failed to write s3://test-bucket/foo))
        end

        it "re-throws the exception" do
          expect { do_write }.to raise_error("something bad happened")
        end
      end

      describe "#delete" do
        let(:do_delete) { subject[key].delete }

        before do
          subject[key].write("bar")
          allow(bucket.objects[key]).to receive(:delete) { fail "something bad happened" }
        end

        it "logs the error" do
          do_delete rescue nil
          expect(log_output).to include(%(failed to delete s3://test-bucket/foo))
        end

        it "re-throws the exception" do
          expect { do_delete }.to raise_error("something bad happened")
        end
      end
    end
  end

  context "with multiple buckets" do

    let(:bucket_names) { %w(bucket1 bucket2 bucket3) }

    subject do
      described_class.new(bucket_names, {}, logger)
    end

    let(:primary_bucket) { fake_s3.buckets["bucket1"] }
    let(:secondary_buckets) { [fake_s3.buckets["bucket2"], fake_s3.buckets["bucket3"]] }

    before do
      @background_jobs = []
      allow(Thread).to receive(:new) do |&block|
        @background_jobs << block
      end
    end

    def run_background_jobs
      @background_jobs.each do |block|
        block.call rescue nil
      end
    end

    context "when writing or deleting objects" do

      let(:key) { "data/file" }
      let(:content) { "CONTENT" }

      before do
        subject[key].write(content)
      end

      describe "#write" do
        it "writes to the first bucket" do
          expect(primary_bucket.objects[key].read).to eq(content)
        end

        it "doesn't write to the other buckets" do
          secondary_buckets.each do |bucket_name|
            expect(bucket_name.objects[key]).to_not exist
          end
        end
      end

      describe "#delete" do
        before do
          run_background_jobs # write to other buckets
          subject[key].delete
        end

        it "deletes from the first bucket" do
          expect(primary_bucket.objects[key]).to_not exist
        end

        it "doesn't delete from the other buckets" do
          secondary_buckets.each do |bucket_name|
            expect(bucket_name.objects[key]).to exist
          end
        end
      end

      context "asynchronously" do
        let(:inaccessible_bucket) { secondary_buckets.first }
        let(:accessible_bucket) { secondary_buckets.last }

        describe "#write" do
          before do
            allow(inaccessible_bucket.objects[key]).to receive(:write) { fail "something bad happened" }
            run_background_jobs
          end

          it "tries to write to the remaining buckets" do
            expect(accessible_bucket.objects[key].read).to eq(content)
          end

          it "logs successful copies" do
            expect(log_output).to include(%(copied s3://bucket1/#{key} to s3://#{accessible_bucket.name}/#{key}))
          end

          it "logs failed copies" do
            expect(log_output).to include(%(failed to copy s3://bucket1/#{key} to s3://#{inaccessible_bucket.name}/#{key}))
          end
        end

        describe "#delete" do
          before do
            run_background_jobs # write to the other buckets
            subject[key].delete

            allow(inaccessible_bucket.objects[key]).to receive(:delete) { fail "something bad happened" }
            run_background_jobs
          end

          it "deletes from the remaining buckets" do
            expect(accessible_bucket.objects[key]).to_not exist
          end

          it "logs successful delete" do
            expect(log_output).to include(%(deleted s3://#{accessible_bucket.name}/#{key}))
          end

          it "logs failed delete" do
            expect(log_output).to include(%(failed to delete s3://#{inaccessible_bucket.name}/#{key}))
          end
        end
      end

    end

  end

end
