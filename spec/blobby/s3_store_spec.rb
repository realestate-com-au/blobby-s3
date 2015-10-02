require "aws-sdk-resources"
require "blobby/s3_store"

# Load the abstract "Store" tests from "blobby".
# This depends on the gem being packaged with "spec" dir intact.
$LOAD_PATH << Gem.loaded_specs["blobby"].full_gem_path + "/spec"

require "blobby/store_behaviour"

describe Blobby::S3Store, :integration => true do

  before(:all) do
    unless ENV.key?("AWS_ACCESS_KEY_ID")
      fail "No AWS credentials provided"
    end
  end

  context "with a writable bucket" do

    EXISTING_BUCKET_NAME = "fake-aws-sdk-s3-test"

    let(:s3_resource) { Aws::S3::Resource.new(:region => "us-east-1")}
    let(:bucket) { s3_resource.bucket(EXISTING_BUCKET_NAME) }

    before do
      bucket.clear!
    end

    subject do
      described_class.new(EXISTING_BUCKET_NAME)
    end

    it_behaves_like Blobby::Store

    describe "#write" do

      let(:key) { "data/file" }
      let(:content) { "CONTENT" }

      before do
        subject[key].write(content)
      end

      it "stores stuff in S3" do
        expect(bucket.object(key).get.body.read).to eq(content)
      end

    end

    describe "#delete" do

      let(:key) { "my_key" }

      before do
        subject[key].write("content")
        subject[key].delete
      end

      it "removes stuff from S3" do
        expect(bucket.object(key)).to_not exist
      end

    end

  end

  context "when we can't talk to S3" do

    let(:bogus_credentials) do
      {
        :access_key_id => "bogus",
        :secret_access_key => "bogus"
      }
    end

    subject do
      described_class.new(EXISTING_BUCKET_NAME, bogus_credentials)
    end

    it { is_expected.not_to be_available }

  end

  context "when the bucket does not exist" do

    BOGUS_BUCKET_NAME = "bogusmcbogusness"

    subject do
      described_class.new(BOGUS_BUCKET_NAME)
    end

    it { is_expected.not_to be_available }

  end

end
