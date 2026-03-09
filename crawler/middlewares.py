# 中间件（处理随机 User-Agent、代理池）

class RandomUserAgentMiddleware:
    def process_request(self, request, spider):
        # TODO: Set random User-Agent
        pass

class ProxyMiddleware:
    def process_request(self, request, spider):
        # TODO: Set proxy
        pass
