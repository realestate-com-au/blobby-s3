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

end
