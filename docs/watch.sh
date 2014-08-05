BUILDDIR="_build"
SPHINXOPTS=
ALLSPHINXOPTS="-d ${BUILDDIR}/doctrees ${SPHINXOPTS} ."
SPHINXBUILD="sphinx-build"
sphinx-build -b html ${ALLSPHINXOPTS} ${BUILDDIR}/html
