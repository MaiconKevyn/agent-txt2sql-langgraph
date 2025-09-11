# SIH-RS Database Structure Documentation

## Overview
This document provides a comprehensive analysis of the PostgreSQL database structure for the SIH-RS (Sistema de Informações Hospitalares - Rio Grande do Sul) database. The database contains 15 tables with a total of over 11 million hospitalization records and related data.

**Database Connection:** postgresql://postgres@localhost:5432/sih_rs (set password via .env DATABASE_URL)

---

## Database Summary
- **Total Tables:** 15
- **Total Records:** ~11.02 million primary hospitalization records
- **Main Entity:** Hospitalization records (internacoes table)
- **Geographic Scope:** Brazilian municipalities (primarily Rio Grande do Sul state)
- **Data Domain:** Healthcare hospitalization information system

---

## Table Structure and Analysis

### 1. INTERNACOES (Main Table)
**Purpose:** Core hospitalization records table containing detailed information about hospital admissions

**Structure:**
- **Rows:** 11,022,199 records
- **Columns:** 26 columns
- **Primary Key:** N_AIH (Hospital admission authorization number)
- **Foreign Keys:** 5 relationships

**Key Columns:**
- `N_AIH` (text, NOT NULL): Unique hospital admission authorization number
- `CNES` (text): Hospital identifier → References hospital.CNES
- `DT_INTER` (timestamp): Admission date
- `DT_SAIDA` (timestamp): Discharge date
- `QT_DIARIAS` (integer): Number of daily stays
- `PROC_REA` (text): Procedure performed code
- `VAL_SH` (double precision): Hospital service value
- `VAL_SP` (double precision): Professional service value
- `VAL_TOT` (double precision): Total value
- `DIAS_PERM` (integer): Days of stay
- `COMPLEX` (text): Complexity level
- `DIAG_PRINC` (text): Primary diagnosis → References cid10.CID
- `DIAG_SECUN` (text): Secondary diagnosis → References cid10.CID
- `CID_NOTIF` (text): Notification CID code → References cid10.CID
- `CID_ASSO` (text): Associated CID code → References cid10.CID
- `MUNIC_MOV` (text): Municipality of movement
- `MUNIC_RES` (text): Municipality of residence
- `NASC` (timestamp): Birth date
- `SEXO` (integer): Gender (coded)
- `IDADE` (double precision): Age
- `NACIONAL` (text): Nationality code
- `NUM_FILHOS` (integer): Number of children
- `RACA_COR` (text): Race/ethnicity code

**Sample Data:**
- AIH: 4316103450186, Date: 2016-07-22, Procedure: 0404010032, Diagnosis: J350
- AIH: 4310100786662, Date: 2010-01-24, Procedure: 0303100044, Diagnosis: O141

---

### 2. MORTES (Deaths)
**Purpose:** Records deaths that occurred during hospitalization

**Structure:**
- **Rows:** 569,405 records
- **Columns:** 2 columns
- **Primary Key:** N_AIH
- **Foreign Keys:** 1 relationship to internacoes

**Key Columns:**
- `N_AIH` (text, NOT NULL): Hospital admission authorization number → References internacoes.N_AIH
- `CID_MORTE` (text): Death cause CID code

**Sample Data:**
- Death records show CID codes like "G459" or "0" (unknown/unspecified)

---

### 3. PROCEDIMENTOS (Procedures)
**Purpose:** Master table for medical procedure codes and descriptions

**Structure:**
- **Rows:** 5,394 procedures
- **Columns:** 2 columns
- **Primary Key:** PROC_REA
- **Foreign Keys:** None (reference table)

**Key Columns:**
- `PROC_REA` (text, NOT NULL): Procedure code
- `NOME_PROC` (text): Procedure description

**Sample Data:**
- "101010010": "ATIVIDADE EDUCATIVA / ORIENTACAO EM GRUPO NA ATENCAO PRIMARIA"
- "101010028": "ATIVIDADE EDUCATIVA / ORIENTACAO EM GRUPO NA ATENCAO ESPECIALIZADA"

---

### 4. CID10 (International Disease Classification)
**Purpose:** Master table for ICD-10 disease codes and descriptions

**Structure:**
- **Rows:** 14,254 disease codes
- **Columns:** 2 columns
- **Primary Key:** CID
- **Foreign Keys:** None (reference table)

**Key Columns:**
- `CID` (text, NOT NULL): ICD-10 disease code
- `CD_DESCRICAO` (text): Disease description

**Sample Data:**
- "P545": "Hemorragia cutanea neonatal"
- "Y085": "Areas de comercio e de servicos"
- "W434": "Rua e estrada"

---

### 5. HOSPITAL
**Purpose:** Master table for hospital information

**Structure:**
- **Rows:** 366 hospitals
- **Columns:** 4 columns
- **Primary Key:** CNES
- **Foreign Keys:** None (reference table)

**Key Columns:**
- `CNES` (text, NOT NULL): National Registry of Health Establishments code
- `NATUREZA` (text): Hospital nature/type
- `GESTAO` (text): Management type
- `NAT_JUR` (text): Legal nature

**Sample Data:**
- CNES codes like "0104523", "0181927", "2223538"

---

### 6. MUNICIPIOS (Municipalities)
**Purpose:** Master table for Brazilian municipalities

**Structure:**
- **Rows:** 5,570 municipalities
- **Columns:** 6 columns
- **Primary Key:** codigo_6d
- **Foreign Keys:** None (reference table)

**Key Columns:**
- `codigo_6d` (text, NOT NULL): 6-digit municipality code
- `codigo_ibge` (text, UNIQUE): IBGE municipality code
- `nome` (text): Municipality name
- `latitude` (double precision): Geographic latitude
- `longitude` (double precision): Geographic longitude
- `estado` (text): State abbreviation

**Sample Data:**
- Abadia de Goiás (GO): -16.7573, -49.4412
- Abadia dos Dourados (MG): -18.4831, -47.3916

---

### 7. DADO_IBGE (IBGE Socioeconomic Data)
**Purpose:** Comprehensive socioeconomic indicators for municipalities

**Structure:**
- **Rows:** 5,570 municipalities
- **Columns:** 22 indicators
- **Primary Key:** codigo_municipio_completo
- **Foreign Keys:** 1 relationship to municipios

**Key Columns:**
- `codigo_municipio_completo` (text, NOT NULL): Complete municipality code → References municipios.codigo_ibge
- `nome_municipio` (text): Municipality name
- `populacao` (double precision): Population (in thousands)
- `densidade_demografica` (double precision): Demographic density
- `salario_medio` (double precision): Average salary
- `pessoal_ocupado` (double precision): Employed persons percentage
- `ideb_anos_iniciais_ensino_fundamental` (double precision): IDEB score (early elementary)
- `ideb_anos_finais_ensino_fundamental` (double precision): IDEB score (late elementary)
- `receita_bruta` (double precision): Gross revenue
- `mortalidade_infantil` (double precision): Infant mortality rate
- `area_cidade` (double precision): City area

---

### 8. UTI_DETALHES (ICU Details)
**Purpose:** Specific information about ICU stays during hospitalization

**Structure:**
- **Rows:** 81,287 ICU records
- **Columns:** 5 columns
- **Primary Key:** N_AIH
- **Foreign Keys:** 1 relationship to internacoes

**Key Columns:**
- `N_AIH` (text, NOT NULL): Hospital admission authorization → References internacoes.N_AIH
- `UTI_MES_TO` (integer): ICU total monthly days
- `MARCA_UTI` (text): ICU marker/type
- `UTI_INT_TO` (integer): ICU total intermediate days
- `VAL_UTI` (double precision): ICU total value

**Sample Data:**
- ICU values ranging from ~21,000 to ~27,000 monetary units

---

### 9. CONDICOES_ESPECIFICAS (Special Conditions)
**Purpose:** Records special medical conditions during hospitalization

**Structure:**
- **Rows:** 1,118,626 records
- **Columns:** 2 columns
- **Primary Key:** N_AIH
- **Foreign Keys:** 1 relationship to internacoes

**Key Columns:**
- `N_AIH` (text, NOT NULL): Hospital admission authorization → References internacoes.N_AIH
- `IND_VDRL` (text): VDRL indicator (syphilis test)

---

### 10. OBSTETRICOS (Obstetric Information)
**Purpose:** Obstetric-specific information for maternity cases

**Structure:**
- **Rows:** 376,202 records
- **Columns:** 2 columns
- **Primary Key:** N_AIH
- **Foreign Keys:** 1 relationship to internacoes

**Key Columns:**
- `N_AIH` (text, NOT NULL): Hospital admission authorization → References internacoes.N_AIH
- `INSC_PN` (text): Prenatal registration number

---

### 11. INSTRUCAO (Education Level)
**Purpose:** Educational level information for patients

**Structure:**
- **Rows:** 68,389 records
- **Columns:** 2 columns
- **Primary Key:** N_AIH
- **Foreign Keys:** 1 relationship to internacoes

**Key Columns:**
- `N_AIH` (text, NOT NULL): Hospital admission authorization → References internacoes.N_AIH
- `INSTRU` (text): Education level code

---

### 12. VINCPREV (Social Security Link)
**Purpose:** Social security/pension system linkage information

**Structure:**
- **Rows:** 6,461 records
- **Columns:** 2 columns
- **Primary Key:** N_AIH
- **Foreign Keys:** 1 relationship to internacoes

**Key Columns:**
- `N_AIH` (text, NOT NULL): Hospital admission authorization → References internacoes.N_AIH
- `VINCPREV` (text): Social security link indicator

---

### 13. CBOR (Professional Classification)
**Purpose:** Brazilian Occupation Classification for professionals

**Structure:**
- **Rows:** 6,461 records
- **Columns:** 2 columns
- **Primary Key:** N_AIH
- **Foreign Keys:** 1 relationship to internacoes

**Key Columns:**
- `N_AIH` (text, NOT NULL): Hospital admission authorization → References internacoes.N_AIH
- `CBOR` (text): Professional classification code

**Sample Data:**
- Codes like "225125", "225250" (likely medical professional categories)

---

### 14. INFEHOSP (Hospital Infections)
**Purpose:** Hospital-acquired infection information

**Structure:**
- **Rows:** 0 records (empty table)
- **Columns:** 2 columns
- **Primary Key:** N_AIH
- **Foreign Keys:** 1 relationship to internacoes

**Key Columns:**
- `N_AIH` (text, NOT NULL): Hospital admission authorization → References internacoes.N_AIH
- `INFEHOSP` (text): Hospital infection indicator

---

### 15. DIAGNOSTICOS_SECUNDARIOS (Secondary Diagnoses)
**Purpose:** Additional secondary diagnoses for hospitalizations

**Structure:**
- **Rows:** 0 records (empty table)
- **Columns:** 3 columns
- **Primary Key:** N_AIH, ordem_diagnostico
- **Foreign Keys:** None

**Key Columns:**
- `N_AIH` (text, NOT NULL): Hospital admission authorization
- `codigo_cid_secundario` (text): Secondary CID code
- `ordem_diagnostico` (text, NOT NULL): Diagnosis order

---

## Database Relationships

### Primary Relationships
1. **internacoes** (N_AIH) ← **mortes** (N_AIH)
2. **internacoes** (N_AIH) ← **uti_detalhes** (N_AIH)
3. **internacoes** (N_AIH) ← **condicoes_especificas** (N_AIH)
4. **internacoes** (N_AIH) ← **obstetricos** (N_AIH)
5. **internacoes** (N_AIH) ← **instrucao** (N_AIH)
6. **internacoes** (N_AIH) ← **vincprev** (N_AIH)
7. **internacoes** (N_AIH) ← **cbor** (N_AIH)
8. **internacoes** (N_AIH) ← **infehosp** (N_AIH)

### Reference Relationships
1. **hospital** (CNES) ← **internacoes** (CNES)
2. **cid10** (CID) ← **internacoes** (DIAG_PRINC, DIAG_SECUN, CID_NOTIF, CID_ASSO)
3. **municipios** (codigo_ibge) ← **dado_ibge** (codigo_municipio_completo)

## Data Quality Notes

### Populated Tables (with data)
- **internacoes**: 11,022,199 records (main table)
- **mortes**: 569,405 records (5.2% of hospitalizations resulted in death)
- **condicoes_especificas**: 1,118,626 records (10.1% of hospitalizations)
- **obstetricos**: 376,202 records (3.4% of hospitalizations)
- **uti_detalhes**: 81,287 records (0.7% of hospitalizations required ICU)
- **instrucao**: 68,389 records (0.6% have education level data)
- **vincprev**: 6,461 records (0.06% have social security data)
- **cbor**: 6,461 records (0.06% have professional classification)

### Empty Tables
- **infehosp**: 0 records (hospital infection tracking not implemented)
- **diagnosticos_secundarios**: 0 records (secondary diagnoses not implemented)

### Reference Tables
- **procedimentos**: 5,394 procedure codes
- **cid10**: 14,254 disease codes
- **hospital**: 366 hospitals
- **municipios**: 5,570 municipalities
- **dado_ibge**: 5,570 municipality socioeconomic data

## Usage Patterns and Insights

1. **Core Entity**: The `internacoes` table is the central fact table containing all hospitalization records
2. **Optional Details**: Most supplementary tables (UTI, obstetric, etc.) contain optional details for specific cases
3. **Geographic Coverage**: Complete Brazilian municipality coverage (5,570 municipalities)
4. **Time Period**: Data spans from 2010 to 2016 based on sample dates
5. **Mortality Rate**: Approximately 5.2% mortality rate based on death records
6. **ICU Usage**: Less than 1% of hospitalizations required ICU care
7. **Maternity Cases**: About 3.4% of hospitalizations are obstetric-related

## Technical Implementation Notes

- All text fields use PostgreSQL `text` type (unlimited length)
- Timestamps stored as `timestamp without time zone`
- Numeric values use `double precision` or `integer` types
- Primary keys are single-column except for `diagnosticos_secundarios` (composite key)
- Foreign key constraints ensure referential integrity
- Unique indexes exist on primary keys and some reference columns

This database structure supports comprehensive analysis of hospitalization patterns, healthcare outcomes, geographic health disparities, and socioeconomic health correlations in Brazil.
