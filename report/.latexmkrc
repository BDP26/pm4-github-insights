# .latexmkrc — latexmk configuration for the GitHub Innovation Portal report
$pdf_mode = 1;
$pdflatex = 'pdflatex -synctex=1 -interaction=nonstopmode -file-line-error %O %S';
$bibtex_use = 1;
$clean_ext = 'synctex.gz synctex(busy) run.xml tex.bak bbl bcf fdb_latexmk run tdo %R-blx.bib';
