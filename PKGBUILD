# Maintainer: Emil Renner Berthing <esmil@mailme.dk>

pkgname=lem-postgres
pkgver=0.3
pkgrel=1
pkgdesc='PostgreSQL library for the Lua Event Machine'
arch=('i686' 'x86_64' 'armv5tel' 'armv7l')
url='https://github.com/esmil/lem-postgres'
license=('GPL')
depends=('lem' 'postgresql-libs')
source=()

build() {
  cd "$startdir"

  make
}

package() {
  cd "$startdir"

  make DESTDIR="$pkgdir/" PREFIX='/usr' install
}

# vim:set ts=2 sw=2 et:
