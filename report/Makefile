.PHONY: main clean FORCE

main: report.pdf

report.pdf: FORCE
	mkdir -p build
	latexmk -pdflatex='lualatex -interaction nonstopmode' -pdf -outdir=build report.tex
	cp build/report.pdf .

clean:
	latexmk -pdf -C -outdir=build
	rm -rf build
	rm -f report.pdf
