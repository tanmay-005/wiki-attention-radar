# Used by the TRANSFORM example in hive/03_analysis.hql.
# Input (tab separated): lang, page_title, views_mobile, views
# Output: lang, page_title, views, mobile_pct, label
BEGIN { FS = "\t"; OFS = "\t" }
$4 > 0 {
  pct = 100.0 * $3 / $4
  label = (pct >= 80) ? "mobile-heavy" : (pct <= 40 ? "desktop-heavy" : "mixed")
  printf "%s\t%s\t%s\t%.1f\t%s\n", $1, $2, $4, pct, label
}
