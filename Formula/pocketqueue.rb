class Pocketqueue < Formula
  desc "Ultra-lightweight embedded message broker supporting Kafka & Redis wire protocols"
  homepage "https://github.com/pandusatrianura/pocketqueue"
  version "0.9.0"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-darwin-arm64.tar.gz"
      sha256 "c66109407a55ed82b1675224ba91ffe17524672226995632a00eef9389599f25"
    else
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-darwin-amd64.tar.gz"
      sha256 "8c3c1d85d14654ba16a37f79685f1371d62b053bc34be466c209e6f064b8d7b2"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-linux-arm64.tar.gz"
      sha256 "a15e748dee4c7b83274493415caade5cba05ab0134c511b78f43f2a90c39374d"
    else
      url "https://github.com/pandusatrianura/pocketqueue/releases/download/v#{version}/pocketqueue-linux-amd64.tar.gz"
      sha256 "9bc39aa1ac4577a2e2b3457192cb1ed08c29899c8b289209e7e6a84736a76165"
    end
  end

  def install
    bin.install "pocketqueue"
  end

  test do
    system "#{bin}/pocketqueue", "--version"
  end
end
