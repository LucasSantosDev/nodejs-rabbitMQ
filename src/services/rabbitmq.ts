import * as amqp from 'amqplib'

class RabbitMQService {
  private connection: amqp.Connection | null = null
  private channel: amqp.Channel | null = null

  constructor(private readonly queue: string) {}

  async connect() {
    try {
      this.connection = await amqp.connect('amqp://guest:guest@localhost')
      this.channel = await this.connection.createChannel()

      await this.channel.assertQueue(this.queue, { durable: true })
    } catch (error) {
      throw new Error(`Connection >>> ${String(error)}`)
    }
  }

  async disconnect() {
    try {
      setTimeout(async () => {
        if (this.connection) {
          await this.connection.close()
          this.connection = null
          this.channel = null
        }
      }, 500)
    } catch (error) {
      throw new Error(`Disconnect >>> ${String(error)}`)
    }
  }

  async sendToQueue(msg: string) {
    try {
      this.checkIfChannelExists('Send to Queue')

      this.channel?.sendToQueue(this.queue, Buffer.from(msg))
    } catch (error) {
      throw new Error(`Send to Queue >>> ${String(error)}`)
    }
  }

  async sendToExchange(exchange: string, routingKey: string, msg: string) {
    try {
      this.checkIfChannelExists('Send to Exchange')

      await this.channel?.assertExchange(exchange, 'direct', { durable: true })
      this.channel?.publish(exchange, routingKey, Buffer.from(msg))
    } catch (error) {
      throw new Error(`Send to Exchange >>> ${String(error)}`)
    }
  }

  async consume() {
    try {
      const messages: Array<any> = []

      this.checkIfChannelExists('Consume')

      await this.channel?.consume(
        this.queue,
        (msg) => {
          if (msg) {
            messages.push(msg.content.toString())

            this.channel?.ack(msg)
          }
        },
        { noAck: false }
      )

      return messages
    } catch (error) {
      throw new Error(`Consume >>> ${String(error)}`)
    }
  }

  private checkIfChannelExists(step: string) {
    if (!this.channel) {
      throw new Error(`${step} >>> Channel is not initialized. Connect first.`)
    }
  }
}

export default RabbitMQService
