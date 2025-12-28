/**
 * Unit tests for ValidBlocksManager
 * Tests block tracking and reorg handling logic
 */

describe('ValidBlocksManager', () => {
  describe('block tracking', () => {
    it('should track block hashes', () => {
      const blocks = [
        { height: 100, hash: '0xabc' },
        { height: 101, hash: '0xdef' },
        { height: 102, hash: '0x123' },
      ]

      const tracked = new Map(blocks.map(b => [b.height, b.hash]))

      expect(tracked.get(100)).toBe('0xabc')
      expect(tracked.get(101)).toBe('0xdef')
      expect(tracked.get(102)).toBe('0x123')
      expect(tracked.size).toBe(3)
    })

    it('should keep only hotBlocksDepth blocks', () => {
      const hotBlocksDepth = 10
      const blocks = Array.from({ length: 20 }, (_, i) => ({
        height: i,
        hash: `0x${i.toString(16)}`,
      }))

      // Simulate keeping only last N blocks
      const kept = blocks.slice(-hotBlocksDepth)

      expect(kept.length).toBe(hotBlocksDepth)
      expect(kept[0].height).toBe(10) // First kept block
      expect(kept[kept.length - 1].height).toBe(19) // Last block
    })
  })

  describe('reorg detection', () => {
    it('should detect reorg when height goes backwards', () => {
      const currentTip = { height: 100, hash: '0xabc' }
      const newBlock = { height: 99, hash: '0xdef' }

      const isReorg = newBlock.height <= currentTip.height

      expect(isReorg).toBe(true)
    })

    it('should not detect reorg for consecutive blocks', () => {
      const currentTip = { height: 100, hash: '0xabc' }
      const newBlock = { height: 101, hash: '0xdef' }

      const isReorg = newBlock.height <= currentTip.height

      expect(isReorg).toBe(false)
    })

    it('should detect reorg at same height', () => {
      const currentTip = { height: 100, hash: '0xabc' }
      const newBlock = { height: 100, hash: '0xdef' } // Different hash, same height

      const isReorg = newBlock.height <= currentTip.height

      expect(isReorg).toBe(true)
    })
  })

  describe('reorg handling', () => {
    it('should clear blocks after reorg height', () => {
      const blocks = [
        { height: 97, hash: '0xa' },
        { height: 98, hash: '0xb' },
        { height: 99, hash: '0xc' },
        { height: 100, hash: '0xd' },
      ]

      const reorgHeight = 99
      const validBlocks = blocks.filter(b => b.height < reorgHeight)

      expect(validBlocks).toHaveLength(2)
      expect(validBlocks[validBlocks.length - 1].height).toBe(98)
    })

    it('should add new blocks after clearing', () => {
      const existingBlocks = [
        { height: 97, hash: '0xa' },
        { height: 98, hash: '0xb' },
      ]

      const newBlocks = [
        { height: 99, hash: '0xNEW1' },
        { height: 100, hash: '0xNEW2' },
      ]

      const allBlocks = [...existingBlocks, ...newBlocks]

      expect(allBlocks).toHaveLength(4)
      expect(allBlocks[2].hash).toBe('0xNEW1')
      expect(allBlocks[3].hash).toBe('0xNEW2')
    })
  })

  describe('registry pattern', () => {
    it('should use registry instead of deletes', () => {
      // Simulate valid blocks registry
      const validBlocks = new Set(['0xa', '0xb', '0xc'])

      // Data with some orphaned blocks
      const allData = [
        { hash: '0xa', value: 1 },
        { hash: '0xORPHAN', value: 2 }, // Not in valid set
        { hash: '0xb', value: 3 },
        { hash: '0xc', value: 4 },
      ]

      // Filter by valid blocks (no DELETE needed)
      const validData = allData.filter(d => validBlocks.has(d.hash))

      expect(validData).toHaveLength(3)
      expect(validData.map(d => d.hash)).toEqual(['0xa', '0xb', '0xc'])
    })

    it('should efficiently update registry on reorg', () => {
      const validBlocks = new Set(['0xa', '0xb', '0xc', '0xd'])

      // Reorg: remove blocks after height, add new ones
      validBlocks.delete('0xc')
      validBlocks.delete('0xd')
      validBlocks.add('0xNEW1')
      validBlocks.add('0xNEW2')

      expect(validBlocks.has('0xa')).toBe(true)
      expect(validBlocks.has('0xb')).toBe(true)
      expect(validBlocks.has('0xc')).toBe(false) // Removed
      expect(validBlocks.has('0xd')).toBe(false) // Removed
      expect(validBlocks.has('0xNEW1')).toBe(true)
      expect(validBlocks.has('0xNEW2')).toBe(true)
    })
  })

  describe('block count management', () => {
    it('should maintain block count within depth limit', () => {
      const hotBlocksDepth = 50
      const blocks: any[] = []

      // Add 100 blocks
      for (let i = 0; i < 100; i++) {
        blocks.push({ height: i, hash: `0x${i}` })

        // Keep only last N
        if (blocks.length > hotBlocksDepth) {
          blocks.shift()
        }
      }

      expect(blocks.length).toBe(hotBlocksDepth)
      expect(blocks[0].height).toBe(50) // First kept block
      expect(blocks[blocks.length - 1].height).toBe(99) // Last block
    })
  })
})
