package subtitles

import (
	"archive/zip"
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func makeZipSel(t *testing.T, entries ...[2]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, e := range entries {
		w, err := zw.Create(e[0])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(e[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func srtCuesSel(lines ...string) string {
	var b strings.Builder
	for i, ln := range lines {
		fmt.Fprintf(&b, "%d\n00:00:0%d,000 --> 00:00:0%d,500\n%s\n\n", i+1, i+1, i+1, ln)
	}
	return b.String()
}

func assCuesSel(lines ...string) string {
	var b strings.Builder
	b.WriteString("[Script Info]\nTitle: f\nScriptType: v4.00+\n\n[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n")
	for i, ln := range lines {
		fmt.Fprintf(&b, "Dialogue: 0,0:00:0%d.00,0:00:0%d.50,Default,,0,0,0,,%s\n", i+1, i+1, ln)
	}
	return b.String()
}

// Junk primeiro (__MACOSX/._*/.DS_Store/diretório, com cues válidos)
// nunca vence a legenda real.
func TestUnzipZipJunkFirstSkipped(t *testing.T) {
	raw := makeZipSel(t,
		[2]string{"__MACOSX/movie.srt", srtCuesSel("MACJUNK")},
		[2]string{"._movie.srt", srtCuesSel("APPLEDOUBLE")},
		[2]string{".DS_Store", "junk"},
		[2]string{"subs/", ""},
		[2]string{"movie.srt", srtCuesSel("GOOD")},
	)
	got, err := unzipSubtitle(raw, "en")
	if err != nil || !strings.Contains(string(got), "GOOD") {
		t.Fatalf("junk deve ser pulado: %q %v", got, err)
	}
}

// Misto: .ass sem cues úteis cai para o .srt bom seguinte; .ass/.ssa
// válidos são aceitos (retornados crus p/ NormalizeLang converter).
func TestUnzipAssBadThenSrtGoodAndAssAccepted(t *testing.T) {
	raw := makeZipSel(t,
		[2]string{"movie.ass", "[Script Info]\nTitle: empty\n"},
		[2]string{"movie.srt", srtCuesSel("SRTBOM")},
	)
	got, err := unzipSubtitle(raw, "en")
	if err != nil || !strings.Contains(string(got), "SRTBOM") {
		t.Fatalf("ass-ruim deve cair p/ srt-bom: %q %v", got, err)
	}
	for _, ext := range []string{".ass", ".ssa"} {
		raw := makeZipSel(t, [2]string{"movie.pt-br" + ext, assCuesSel("Olá mundo")})
		got, err := unzipSubtitle(raw, "pt-BR")
		if err != nil {
			t.Fatalf("%s: %v", ext, err)
		}
		srt, _, err := NormalizeLang(got, "pt-BR")
		if err != nil || !strings.Contains(srt, "Olá mundo") {
			t.Fatalf("%s convertido: %q %v", ext, srt, err)
		}
	}
}

// Multilíngue fora de ordem: entrada do idioma do candidato vence
// formato e nº de cues das outras; delimitado, não substring solta.
func TestUnzipMultilingualOutOfOrder(t *testing.T) {
	raw := makeZipSel(t,
		[2]string{"movie.es.srt", srtCuesSel("ES1", "ES2", "ES3")},
		[2]string{"movie.pt-br.ass", assCuesSel("PTBR1", "PTBR2")},
		[2]string{"movie.en.srt", srtCuesSel("EN1")},
	)
	got, err := unzipSubtitle(raw, "en")
	if err != nil || !strings.Contains(string(got), "EN1") {
		t.Fatalf("en deve vencer fora de ordem: %q %v", got, err)
	}
	got, err = unzipSubtitle(raw, "pt-BR")
	if err != nil || !strings.Contains(string(got), "PTBR1") {
		t.Fatalf("pt-br .ass deve vencer .srt de outro idioma: %q %v", got, err)
	}
	// "english" contém "en" como substring solta: não é match delimitado,
	// então o genérico com mais cues vence o stub de 1 cue.
	raw = makeZipSel(t,
		[2]string{"movie.english.srt", srtCuesSel("STUB")},
		[2]string{"movie.srt", srtCuesSel("GEN1", "GEN2", "GEN3")},
	)
	got, err = unzipSubtitle(raw, "en")
	if err != nil || !strings.Contains(string(got), "GEN3") {
		t.Fatalf("substring solta não pode casar idioma: %q %v", got, err)
	}
	for _, name := range []string{"movie.pob.srt", "movie.pt_br.srt", "movie.pt-br.srt"} {
		raw := makeZipSel(t,
			[2]string{"movie.srt", srtCuesSel("GENERIC")},
			[2]string{name, srtCuesSel("PTBRNAMED")},
		)
		got, err := unzipSubtitle(raw, "pt-BR")
		if err != nil || !strings.Contains(string(got), "PTBRNAMED") {
			t.Fatalf("%s deve casar pt-BR delimitado: %q %v", name, got, err)
		}
	}
}

// Formato vs cues: empate mantém .srt; mais cues parseados vencem
// formato; "-->" no texto não infla a contagem (mutação anti-Count).
func TestUnzipFormatTieAndParsedCueCount(t *testing.T) {
	raw := makeZipSel(t,
		[2]string{"movie.srt", srtCuesSel("ONLYONE")},
		[2]string{"movie.ass", assCuesSel("FULL1", "FULL2", "FULL3")},
	)
	got, err := unzipSubtitle(raw, "en")
	if err != nil || !strings.Contains(string(got), "FULL3") {
		t.Fatalf(".ass completo deve vencer stub .srt: %q %v", got, err)
	}
	raw = makeZipSel(t,
		[2]string{"movie.ass", assCuesSel("ASS ONE")},
		[2]string{"movie.srt", srtCuesSel("SRTONE")},
	)
	got, err = unzipSubtitle(raw, "en")
	if err != nil || !strings.Contains(string(got), "SRTONE") {
		t.Fatalf("empate de cues deve manter .srt: %q %v", got, err)
	}
	raw = makeZipSel(t,
		[2]string{"movie.srt", srtCuesSel("SRTARROW --> --> --> -->")},
		[2]string{"movie.ass", assCuesSel("ASSUM", "ASSDOIS", "ASSTRES")},
	)
	got, err = unzipSubtitle(raw, "en")
	if err != nil || !strings.Contains(string(got), "ASSTRES") || strings.Contains(string(got), "SRTARROW") {
		t.Fatalf("cues parseados, não setas no texto: %q %v", got, err)
	}
}

// >16MiB por entrada: erro explícito, nunca truncamento silencioso.
func TestUnzipOversizedEntryExplicitError(t *testing.T) {
	big := makeZipSel(t, [2]string{"big.srt", strings.Repeat("x", (16<<20)+1)})
	if _, err := unzipSubtitle(big, "en"); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf(">16MiB deve ser erro 'too large', got %v", err)
	}
}

// Orçamento agregado: 16 entradas tentadas / 32MiB expandidos.
func TestUnzipBudgetManyEntries(t *testing.T) {
	var entries [][2]string
	for i := 0; i < 20; i++ {
		entries = append(entries, [2]string{fmt.Sprintf("junk%02d.srt", i), "not a subtitle at all"})
	}
	entries = append(entries, [2]string{"good.srt", srtCuesSel("TOOFAR")})
	raw := makeZipSel(t, entries...)
	if _, err := unzipSubtitle(raw, "en"); err == nil || !strings.Contains(err.Error(), "budget") {
		t.Fatalf("muitas entradas devem esgotar o orçamento, got %v", err)
	}
	bigJunk := strings.Repeat("x", 5<<20)
	var entries2 [][2]string
	for i := 0; i < 8; i++ {
		entries2 = append(entries2, [2]string{fmt.Sprintf("big%02d.srt", i), bigJunk})
	}
	entries2 = append(entries2, [2]string{"good.srt", srtCuesSel("TOOFARBYTES")})
	raw = makeZipSel(t, entries2...)
	if _, err := unzipSubtitle(raw, "en"); err == nil || !strings.Contains(err.Error(), "budget") {
		t.Fatalf("orçamento de bytes expandidos deve parar antes da boa, got %v", err)
	}
}
