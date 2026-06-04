#!/bin/sh
set -eu

REPO="eiennohito/dsless"
INSTALL_DIR="${DSLESS_INSTALL_DIR:-$HOME/.local/bin}"

OS=$(uname -s)
ARCH=$(uname -m)

case "$OS" in
  Darwin)
    ASSET="dsless-darwin-universal.tar.gz"
    ;;
  Linux)
    case "$ARCH" in
      x86_64|amd64)  ASSET="dsless-linux-x86_64.tar.gz" ;;
      aarch64|arm64) ASSET="dsless-linux-aarch64.tar.gz" ;;
      *) echo "Error: unsupported architecture: $ARCH" >&2; exit 1 ;;
    esac
    ;;
  *)
    echo "Error: unsupported OS: $OS" >&2; exit 1
    ;;
esac

VERSION="${DSLESS_VERSION:-latest}"
if [ "$VERSION" = "latest" ]; then
  URL="https://github.com/$REPO/releases/latest/download/$ASSET"
else
  URL="https://github.com/$REPO/releases/download/$VERSION/$ASSET"
fi

mkdir -p "$INSTALL_DIR"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "Downloading dsless ($OS $ARCH)..."
curl -fsSL "$URL" -o "$TMP/$ASSET"
tar xzf "$TMP/$ASSET" -C "$TMP"
mv "$TMP/dsless" "$INSTALL_DIR/dsless"
chmod +x "$INSTALL_DIR/dsless"

echo "Installed dsless to $INSTALL_DIR/dsless"

case ":${PATH}:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    echo ""
    echo "Add $INSTALL_DIR to your PATH:"
    echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
    ;;
esac
