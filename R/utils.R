library(data.table)
library(arrow)
library(stringr)

METHODOLOGY_VERSION <- "1.0.0"

VPS_NOMINEE_ORGNRS <- c(
  "884709932",
  "986252344",
  "977074010",
  "912507419",
  "981276957"
)

NOMINEE_NAME_PATTERNS <- c(
  "NOMINEE",
  "FORVALTNING",
  "SECURITIES OSLO",
  "VERDIPAPIRSENTRALEN",
  "EURONEXT SECURITIES"
)

ROLE_WEIGHT <- c(
  SIGN = 3, PROK = 3,
  DAGL = 2, LEDE = 2, NEST = 2, DTPR = 2,
  MEDL = 1, VARA = 1, OBS = 1, FFØR = 1, REVI = 1, KOMP = 1, KDIR = 1
)

is_nominee <- function(holder_id, holder_name = NA_character_) {
  if (is.na(holder_id)) return(FALSE)
  if (holder_id %in% VPS_NOMINEE_ORGNRS) return(TRUE)
  if (!is.na(holder_name)) {
    upper_name <- str_to_upper(holder_name)
    if (any(str_detect(upper_name, NOMINEE_NAME_PATTERNS))) return(TRUE)
  }
  FALSE
}

filter_nominees_dt <- function(dt) {
  upper_names <- str_to_upper(dt$holder_name)
  pat <- paste(NOMINEE_NAME_PATTERNS, collapse = "|")
  keep <- !(dt$holder_id %in% VPS_NOMINEE_ORGNRS) &
          (is.na(upper_names) | !str_detect(upper_names, pat))
  dt[keep]
}

gcs_path <- function(rel) paste0("gs://sondre_brreg_data/", rel)
