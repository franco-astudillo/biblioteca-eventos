import { Channel } from 'amqplib';

export const EXCHANGES = {
  eventos: { nombre: 'biblioteca.eventos', tipo: 'topic' },
  comandos: { nombre: 'biblioteca.comandos', tipo: 'direct' },
  dlx: { nombre: 'biblioteca.dlx', tipo: 'direct' },
} as const;

export const COLAS = {
  notificaciones: 'notificaciones',
  auditoria: 'auditoria',
  correos: 'correos',
} as const;

export const ROUTING_KEYS = {
  prestamoCreado: 'prestamo.creado',
  prestamoDevuelto: 'prestamo.devuelto',
  libroAgotado: 'libro.agotado',
  correoEnviar: 'correo.enviar',
} as const;

export const BINDINGS = [
  { cola: COLAS.notificaciones, exchange: EXCHANGES.eventos.nombre, patron: 'prestamo.*' },
  { cola: COLAS.auditoria, exchange: EXCHANGES.eventos.nombre, patron: '#' },
  { cola: COLAS.correos, exchange: EXCHANGES.comandos.nombre, patron: ROUTING_KEYS.correoEnviar },
] as const;

export async function declararTopologia(canal: Channel): Promise<void> {
  for (const exchange of Object.values(EXCHANGES)) {
    await canal.assertExchange(exchange.nombre, exchange.tipo, { durable: true });
  }
  for (const cola of Object.values(COLAS)) {
    await canal.assertQueue(cola, { durable: true });
  }
  for (const binding of BINDINGS) {
    await canal.bindQueue(binding.cola, binding.exchange, binding.patron);
  }
}
