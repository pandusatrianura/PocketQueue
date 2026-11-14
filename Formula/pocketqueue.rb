class Pocketqueue < Formula
  desc "Ultra-lightweight embedded message broker supporting Kafka & Redis wire protocols"
  homepage "https://github.com/pandusatrianura/pocketqueue"
  version "0.3.0"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-darwin-arm64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000"
    else
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-darwin-amd64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-linux-arm64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000"
    else
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-linux-amd64.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000"
    end
  end

  def install
    bin.install "pocketqueue"
  end

  test do
    system "#{bin}/pocketqueue", "--version"
  end
end
