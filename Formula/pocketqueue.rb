class Pocketqueue < Formula
  desc "Ultra-lightweight embedded message broker supporting Kafka & Redis wire protocols"
  homepage "https://github.com/pandusatrianura/pocketqueue"
  version "0.8.0"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-darwin-arm64.tar.gz"
      sha256 "9548c2aba50fb033bcd9065082864a3f57c67e924ee6ffab68aa05ef8259195e"
    else
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-darwin-amd64.tar.gz"
      sha256 "c00c64e6f47698b1bd0a7fabc0ce6a100e24e6a08470d960f459c60a1a62e6cd"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-linux-arm64.tar.gz"
      sha256 "16a1ac9f61e3c0710d17a928d54c55ed9703088b33e975925b17732fa809e720"
    else
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-linux-amd64.tar.gz"
      sha256 "45a2e47b342f1beb0aba807ae09b1fd81aec2916d6899080137a598859d751c9"
    end
  end

  def install
    bin.install "pocketqueue"
  end

  test do
    system "#{bin}/pocketqueue", "--version"
  end
end
