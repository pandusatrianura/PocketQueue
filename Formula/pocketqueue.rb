class Pocketqueue < Formula
  desc "Ultra-lightweight embedded message broker supporting Kafka & Redis wire protocols"
  homepage "https://github.com/pandusatrianura/pocketqueue"
  version "0.7.0"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-darwin-arm64.tar.gz"
      sha256 "67955aa2e48242864712929fd41c416b2095b651360d36485b190ea614d8965f"
    else
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-darwin-amd64.tar.gz"
      sha256 "9e28eeca8ef00a89f4ce7684a0b51f46995986e85e2f1f4be48e19c8b5d2a1c5"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-linux-arm64.tar.gz"
      sha256 "d6fcdf1cab5b122109945549dd0523b2cc5af56d1d68b203876102d8c1268df3"
    else
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-linux-amd64.tar.gz"
      sha256 "a7c62c0c0cab2e95ee83131997c41a847c56e3bd25c162b791ded74fb6de7c02"
    end
  end

  def install
    bin.install "pocketqueue"
  end

  test do
    system "#{bin}/pocketqueue", "--version"
  end
end
