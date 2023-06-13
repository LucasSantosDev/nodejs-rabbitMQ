import express, { Request, Response } from 'express'
import { generateUUID } from './helpers/utils'
import RabbitMQService from './services/rabbitmq'

const queueName = 'queue-test'

const router = express.Router()

router.get('/publish', async (req: Request, res: Response) => {
  const message = `Message - ${generateUUID()}`

  const rabbitMQService = new RabbitMQService(queueName)
  await rabbitMQService.connect()
  await rabbitMQService.sendToQueue(message)
  await rabbitMQService.disconnect()

  res.json({ message })
})

router.get('/consume', async (req: Request, res: Response) => {
  const rabbitMQService = new RabbitMQService(queueName)
  await rabbitMQService.connect()
  const messages = await rabbitMQService.consume()
  await rabbitMQService.disconnect()

  res.json({ messages })
})

export default router
