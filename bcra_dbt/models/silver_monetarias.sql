with base as (

  select
    id,
    fecha,
    variable,
    case
      when variable = 'BADLAR en pesos de bancos privados (en % e.a.)' then 'BADLAR en pesos de bancos privados'
      when variable = 'Base monetaria - Total (en millones de pesos)' then 'Base monetaria - Total'
      when variable = 'Circulacion monetaria (en millones de pesos)' then 'Circulacion monetaria'
      when variable = 'Inflacion interanual (variacion en % i.a.)' then 'Inflacion interanual'
      when variable = 'Inflacion mensual (variacion en %)' then 'Inflacion mensual'
      when variable = 'Reservas Internacionales del BCRA (en millones de dolares - cifras provisorias sujetas a cambio de valuacion)' then 'Reservas Internacionales del BCRA'
      when variable = 'Tipo de Cambio Mayorista ($ por USD) Comunicacion A 3500 - Referencia' then 'Tipo de Cambio Mayorista ($ por USD)'
      when variable = 'Tipo de Cambio Minorista ($ por USD) Comunicacion B 9791 - Promedio vendedor' then 'Tipo de Cambio Minorista ($ por USD)'
      when variable = 'Unidad de Vivienda (UVI) (en pesos -con dos decimales-, base 31.3.2016=14.05)' then 'Unidad de Vivienda (UVI)'
      else variable
    end as variable_limpia,
    valor

  from bcra_raw.public.monetarias_raw

)

select
  id,
  fecha,
  variable_limpia as variable,
  valor

from base
