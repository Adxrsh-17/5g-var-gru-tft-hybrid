# 5G VAR–GRU–TFT Hybrid: Unified Traffic Analytics & Forecasting Pipeline

## Overview

**5g-var-gru-tft-hybrid** provides a comprehensive pipeline for analytics and forecasting of 5G slice traffic, leveraging packet capture (PCAP)-derived Key Performance Indicators (KPIs). This project synthesizes classical time series models (VAR), deep learning architectures (GRU), and state-of-the-art interpretable models (TFT) in a hybrid forecaster, following NWDAF (Network Data Analytics Function) fusion frameworks, to deliver advanced and actionable insights for 5G network management.

## Key Features

- **End-to-End Data Pipeline**  
  Automated extraction, transformation, and fusion of KPIs from PCAP files across core 5G service domains:  
  - Enhanced Mobile Broadband (**eMBB**)  
  - Ultra Reliable Low Latency Communication (**URLLC**)  
  - Massive Machine Type Communication (**mMTC**)

- **Multivariate Time Series Construction**  
  Unified datasets engineered for large-scale, granular forecasting tasks.

- **Hybrid Model Integration**  
  Combines:
  - **Vector AutoRegression (VAR)** for multivariate temporal correlations
  - **Gated Recurrent Unit (GRU)** for robust sequence modeling
  - **Temporal Fusion Transformer (TFT)** for interpretability and context fusion

- **Benchmarking & Evaluation**  
  Implements baseline models and systematic evaluation protocols for comparative analysis.

- **NWDAF-style Analytics**  
  Aligned with 3GPP NWDAF standards, enabling network automation, anomaly detection, and predictive resource allocation.

## Typical Applications

- Forecasting slice-specific traffic demand and utilization
- Proactive network orchestration and SLA assurance
- Academic research on time series modeling for telecommunications
- Production-grade deployment in 5G/6G infrastructure analytics

## Repository Structure

```
├── data/          # Example datasets or PCAP-derived KPI samples
├── src/           # Source code for pipeline, models, utilities
├── docs/          # Documentation and architecture diagrams
├── experiments/   # Sample notebooks and benchmark results
└── README.md      # Project overview and instructions
```

## Quick Start

1. **Clone the repository**
   ```sh
   git clone https://github.com/Adxrsh-17/5g-var-gru-tft-hybrid.git
   cd 5g-var-gru-tft-hybrid
   ```
2. **Install requirements**
   ```sh
   # See requirements.txt or `src/` setup modules for dependencies
   pip install -r requirements.txt
   ```
3. **Run data processing and modeling workflows**
   ```sh
   # Example pipeline commands can be provided in docs or scripts
   python src/run_pipeline.py --input_dir data/ --config configs/config.yaml
   ```

Refer to the [documentation](docs/) for detailed usage, configuration, and customization guidelines.

## Model Architecture

- **VAR**: Models lagged relationships among multiple slice KPIs
- **GRU**: Captures sequential data patterns with efficient RNN cells
- **TFT**: Integrates attention, variable selection, and interpretable temporal fusion

All models are orchestrated within a modular forecasting framework, adhering to best practices in time series research and real-world deployment.

## License

This repository is licensed under the [MIT License](LICENSE).

## Citation

If you use this work in academic publications or industry solutions, please cite appropriately or refer to the repository in your documentation.

## Contact & Contributions

For questions, feature requests, or contributions, please open issues or submit pull requests via GitHub.  
Maintainer: [Adxrsh-17](https://github.com/Adxrsh-17)

---

*This project is intended for research, prototyping, and production insights into 5G analytics and is actively maintained. For commercial inquiries, please contact the maintainer.*