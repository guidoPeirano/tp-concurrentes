//! Cálculo del monto a cobrar por un alquiler.

use comun::config::TarifaConfig;
use comun::Timestamp;

/// Monto = tarifa base + (tarifa por minuto × minutos de uso).
pub fn calcular_monto(tarifa: &TarifaConfig, t0: Timestamp, t1: Timestamp) -> f64 {
    let minutos = t0.minutos_hasta(t1);
    tarifa.base + tarifa.por_minuto * minutos as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cobra_base_mas_por_minuto() {
        let tarifa = TarifaConfig {
            base: 50.0,
            por_minuto: 10.0,
        };
        // 2 minutos exactos: 50 + 10*2 = 70.
        let monto = calcular_monto(&tarifa, Timestamp(0), Timestamp(120_000));
        assert_eq!(monto, 70.0);
    }

    #[test]
    fn menos_de_un_minuto_cobra_solo_la_base() {
        let tarifa = TarifaConfig {
            base: 50.0,
            por_minuto: 10.0,
        };
        let monto = calcular_monto(&tarifa, Timestamp(0), Timestamp(30_000));
        assert_eq!(monto, 50.0);
    }
}
