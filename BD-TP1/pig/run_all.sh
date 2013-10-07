#!/bin/bash
# Editar este script para parametrizar cualquier tipo de input y correr todas las metricas juntas

echo "================= Corriendo metrica 1"
pig -param OUTPUT_PATH=g1_pig1_$(date +%s%N) metric1.pig
echo "================= Corriendo metrica 2"
pig -param OUTPUT_PATH=g1_pig2_$(date +%s%N) metric2.pig
echo "================= Corriendo metrica 3"
pig -param OUTPUT_PATH=g1_pig3_$(date +%s%N) metric3.pig
echo "================= Corriendo metrica 4"
pig -param OUTPUT_PATH=g1_pig4_$(date +%s%N) metric4.pig
