# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an R-based mobility and urban planning analysis project for Marseille, France. The codebase implements the MEAPS (Modèle d'Estimation des flux d'Actifs Par Simulation) methodology for estimating commuting flows, analyzing CO2 emissions, and conducting urban accessibility studies.

## Key Directories

- **distances/** - Distance and travel time calculations using multiple transportation modes (car, transit, bike, walk)
- **flux/** - Commuting flux estimation and MEAPS model implementation
- **projection/** - Mobility projection models including loop frequency, modal choice, and detour ratios
- **estimation/** - Gravity model estimation and MEAPS estimation functions
- **deploc/** - Analysis of mobility survey data (Enquête mobilité)
- **access/** - Accessibility analysis across different transportation modes
- **mobilité personnes/** - Personal mobility models and utility functions

## Global Configuration

The project uses a centralized configuration system:

- **mglobals.r** - Defines all global variables, file paths, and parameters. Always source this file first:
  - Data directories: `/space_mounts/data/marseille/` for main data storage
  - Key parameters: `seuil_temps_car` (120 min), `seuil_distance_proba` (35km)
  - File paths for all major datasets (distances, employment, flows, etc.)

- **setup.R** - Initial setup script that loads core libraries and geographic data

## Key Data Structures

- **c200ze** - 200m x 200m grid cells (INSPIRE standard) with population and employment data
- **idINS** - INSPIRE grid cell identifiers used throughout for spatial joins
- **mobpro** - Professional mobility data from national surveys
- **meaps** - MEAPS model output containing estimated commuting flows
- **delta_iris** - IRIS-level mobility projections partitioned by geographic zones

## Core Workflow

### 1. Distance Calculation Pipeline

Scripts in `distances/` follow a numbered sequence:

1. `1 zones.R` - Define geographic zones
2. `1.2 geographie.R` - Process geographic data and administrative boundaries
3. `2 distances r5.R` / `2 distances dgr.r` - Calculate travel times using R5/dodgr routing engines
4. `3 all_mode.R` - Consolidate all transportation modes into unified dataset
5. `3 make idINS file.R` - Create the master idINS reference file

### 2. Flux Estimation

The MEAPS model workflow (`flux/` directory):

1. `make time_matrix.R` - Create time-ranked matrices from distance data
2. `meaps flux.R` - Run the MEAPS multishuf algorithm to estimate flows
3. `pollution.R` - Calculate CO2 emissions from estimated flows

The MEAPS algorithm uses the `rmeaps` package with `multishuf_oc()` function for origin-constrained flow estimation.

### 3. Projection Workflow

`projection/filiere_projection.r` is the main projection pipeline that:

- Loads pre-estimated models (frequency, loop types, modal choice)
- Processes IRIS zones iteratively to calculate mobility patterns
- Outputs partitioned Parquet datasets to `delta_iris/` directory
- Uses DuckDB for efficient data manipulation of large datasets

## Required R Packages

Core packages used extensively:
- **tidyverse** - Data manipulation (always prefer dplyr functions, use `conflicted` package)
- **sf** - Spatial operations
- **arrow** / **parquet** - Large dataset I/O
- **data.table** - High-performance data operations
- **duckdb** - In-process SQL analytics
- **r3035** - INSPIRE grid utilities (idINS functions)
- **rmeaps** - MEAPS flow estimation algorithms
- **accessibility** - Accessibility metrics
- **ofce** - Plotting themes and utilities

## Common Patterns

### Conflict Resolution
Always declare function preferences at the start:
```r
library(conflicted)
conflict_prefer("filter", "dplyr", quiet=TRUE)
conflict_prefer("select", "dplyr", quiet=TRUE)
```

### Spatial Data Processing
- Grid cells use EPSG:3035 projection
- Use `idINS2square()` to convert idINS to geometry
- Use `idINS3035()` to convert coordinates to idINS

### Large Dataset Handling
- Use Arrow datasets with DuckDB backend: `open_dataset(path) |> to_duckdb()`
- Write partitioned Parquet files for IRIS-level data
- Configure DuckDB memory limits in scripts that process large data

### Parallel Processing
```r
plan("multisession", workers = n)
future_walk(...) # for side effects
future_map(...) # for returning results
```

## File Naming Conventions

- Numbered prefixes indicate execution order (e.g., `1 zones.R`, `2 distances r5.R`)
- `f.*.r` files contain function definitions
- `MOD_*.rda` files contain pre-estimated statistical models
- `.qs` files use the qs package for fast R object serialization
- `.parquet` files for tabular data, especially large datasets

## Important Notes

- The project focuses on the Marseille metropolitan area (EPCI code: 200054807)
- Distance calculations are capped at 120 minutes by car
- The codebase uses both 2017 and 2021 administrative boundaries (tracked as `com17` and `com21`)
- SCOT (Schéma de Cohérence Territoriale) zones are used for geographic filtering
- Travel time matrices include 4 modes: walk, bike, car, and transit
- CO2 calculations include a constant term (1.1584 km/day) representing inevitable car usage

## Data Sources

- **mobpro** - Professional mobility from national census
- **mob2019** - 2019 national mobility survey
- **c200** - INSEE 200m grid population data
- **GTFS** - Public transit schedules
- **OSM** - OpenStreetMap road network (stored as PBF files)
