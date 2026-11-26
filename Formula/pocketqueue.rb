class Pocketqueue < Formula
  desc "Ultra-lightweight embedded message broker supporting Kafka & Redis wire protocols"
  homepage "https://github.com/pandusatrianura/pocketqueue"
  version "0.7.0"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-darwin-arm64.tar.gz"
      sha256 "6bb1c9a5939870ed38908ece8911f14a7eb7e3deffa9e6a539ccd13ea70d6edf"
    else
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-darwin-amd64.tar.gz"
      sha256 "d5346bb10aaf081907bff40b677301074e0215843578d8fa2a09fde9e543bbf8"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-linux-arm64.tar.gz"
      sha256 "323ef22096d9d0aa662e56307a657ecca3ce07393131b82270adbdb5985c3186"
    else
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-linux-amd64.tar.gz"
      sha256 "7853090a486bb4b5c3a77e65464291d76d5cd9eaab41d655d3cadd388716d2a5"
    end
  end

  def install
    bin.install "pocketqueue"
  end

  test do
    system "#{bin}/pocketqueue", "--version"
  end
end
