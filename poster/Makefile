.PHONY: main clean FORCE

main: poster.pdf

poster.pdf: FORCE
	mkdir -p build
	latexmk -pdflatex='lualatex -interaction nonstopmode' -pdf -outdir=build poster.tex
	cp build/poster.pdf .

clean:
	latexmk -pdf -C -outdir=build
	rm -rf build
	rm -f poster.pdf
