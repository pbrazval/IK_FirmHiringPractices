
*esttab est1 est1b est1c using r20_tab1.tex, se scalars(N r2 bic) mtitles keep(at ppegt emp sale K_int K_int_Know is_senior)

* q_tot removed, revt removed (collinear with sale)

use "/raid/users/pbrazval/PhDbyTopic/BargainingPower/data/shortjobs_0dums_multimktfirms_wPMN.dta", clear

pwd 

cd PhDbyTopic/BargainingPower/output/fig

pwd

set matsize 5000

rename ppegt Kphy
rename K_int Korg
rename K_int_Know Kknow
rename emp Emp
rename at Assets
rename sale Sales
rename unemp MSA_Unemp
rename dice_metro MSA
rename max_soc occ
rename fyear year
rename LPERMNO firm

eststo est1: reghdfe isnumeric_payrate Kphy Korg Kknow Emp Assets Sales MSA_Unemp, absorb(MSA occ year, savefe) vce(cluster MSA occ)
quietly estadd local fxe "MSA,OCC,Y", replace
quietly estadd local cl "MSA,OCC", replace
eststo est1a: reghdfe isnumeric_payrate Kphy Korg Kknow Emp  Assets Sales MSA_Unemp, absorb(MSA occ year, savefe) vce(cluster MSA ind12)
quietly estadd local fxe "MSA,OCC,Y", replace
quietly estadd local cl "MSA,Ind", replace
eststo est1b: reghdfe isnumeric_payrate Kphy Korg Kknow Emp Assets Sales MSA_Unemp, absorb(i.MSA#i.year occ, savefe) vce(cluster MSA occ)
quietly estadd local fxe "MSA*Y,OCC", replace
quietly estadd local cl "MSA,OCC", replace
eststo est1c: reghdfe isnumeric_payrate Kphy Korg Kknow Emp Assets Sales MSA_Unemp, absorb(i.MSA i.occ#i.year, savefe) vce(cluster MSA occ)
quietly estadd local fxe "MSA,OCC*Y", replace
quietly estadd local cl "MSA,OCC", replace
eststo est1d: reghdfe isnumeric_payrate Kphy Korg Kknow Emp Assets Sales c.MSA_Unemp#c.Korg, absorb(MSA occ year, savefe) vce(cluster MSA occ)
quietly estadd local fxe "MSA,OCC,Y", replace
quietly estadd local cl "MSA,OCC", replace
eststo est1e: reghdfe isnumeric_payrate Kphy Korg Kknow Emp Assets Sales MSA_Unemp, absorb(MSA occ year firm, savefe) vce(cluster MSA occ)
quietly estadd local fxe "MSA,OCC,Y,Fm", replace
quietly estadd local cl "MSA,OCC", replace
eststo est1f: reghdfe isnumeric_payrate Kphy Korg Kknow Emp Assets Sales MSA_Unemp, absorb(MSA occ year ind12, savefe) vce(cluster MSA occ)
quietly estadd local fxe "MSA,OCC,Y,Ind", replace
quietly estadd local cl "MSA,OCC", replace

esttab est1 est1a est1b est1c, replace se s(fxe cl N r2, label("FE" "Clst." "N" "R2")) mtitles keep(Kphy Korg Kknow Emp Assets Sales MSA_Unemp c.MSA_Unemp#c.Korg)
esttab est1 est1d est1e est1f, replace se s(fxe cl N r2, label("FE" "Clst." "N" "R2")) mtitles keep(Kphy Korg Kknow Emp Assets Sales MSA_Unemp)

esttab est1 est1b est1c est1d using r29_tab1.tex, replace se s(fxe cl N r2, label("FE" "Clst." "N" "R2")) mtitles keep(Kphy Korg Kknow Emp Assets Sales MSA_Unemp c.MSA_Unemp#c.Korg)
esttab est1 est1a est1e est1f using r29_tab2.tex, replace se s(fxe cl N r2, label("FE" "Clst." "N" "R2")) mtitles keep(Kphy Korg Kknow Emp Assets Sales MSA_Unemp)

esttab est1 est1b est1c est1d using r29_tab1b.tex, replace se s(fxe cl N r2, label("FE" "Clst." "N" "R2")) mtitles keep(Kphy Korg Kknow MSA_Unemp c.MSA_Unemp#c.Korg)
esttab est1 est1a est1e est1f using r29_tab2b.tex, replace se s(fxe cl N r2, label("FE" "Clst." "N" "R2")) mtitles keep(Kphy Korg Kknow MSA_Unemp)


