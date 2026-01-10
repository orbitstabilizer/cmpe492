.PHONY: main midterm final clean FORCE

main: midterm final

midterm: FORCE
	mkdir -p build
	latexmk -pdflatex='lualatex -interaction nonstopmode' -pdf -outdir=build mt-report.tex
	cp build/mt-report.pdf .

final: FORCE
	mkdir -p build
	latexmk -pdflatex='lualatex -interaction nonstopmode' -pdf -outdir=build final-report.tex
	cp build/final-report.pdf .

clean:
	latexmk -pdf -C -outdir=build
	rm -rf build
	rm -f mt-report.pdf final-report.pdf
