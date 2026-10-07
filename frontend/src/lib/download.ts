/** 浏览器端下载 JSON — 策略导出包走这个, 避免把 700KB 文本先塞进 state 再转存。 */
export function downloadJson(data: unknown, filename: string): number {
  const text = JSON.stringify(data, null, 2)
  const blob = new Blob([text], { type: 'application/json;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  // 立即 revoke 在部分浏览器会中断下载, 延后一拍更稳
  setTimeout(() => URL.revokeObjectURL(url), 4000)
  return blob.size
}
