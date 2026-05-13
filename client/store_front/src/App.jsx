import React, { useMemo, useState, useEffect } from "react";
import { motion } from "framer-motion";
import { ShoppingCart, Search, Star, Plus, Minus } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";
import { Input } from "@/components/ui/input";
import { trackAddToCart, trackRemoveFromCart, initMouseTracking, initTabTracking, stopTracking, trackNavigateAway, configureKinesis, flushNow } from "@/lib/telemetry";

const PRODUCTS = [
  {
    id: 1,
    name: "Minimal Desk Lamp",
    category: "Home",
    price: 49,
    rating: 4.7,
    image: "https://images.unsplash.com/photo-1505693416388-ac5ce068fe85?auto=format&fit=crop&w=900&q=80",
    description: "A clean, modern lamp for workspace mockups.",
  },
  {
    id: 2,
    name: "Everyday Sneakers",
    category: "Fashion",
    price: 89,
    rating: 4.5,
    image: "https://images.unsplash.com/photo-1542291026-7eec264c27ff?auto=format&fit=crop&w=900&q=80",
    description: "Comfortable casual footwear for storefront demos.",
  },
  {
    id: 3,
    name: "Wireless Headphones",
    category: "Electronics",
    price: 129,
    rating: 4.8,
    image: "https://images.unsplash.com/photo-1505740420928-5e560c06d30e?auto=format&fit=crop&w=900&q=80",
    description: "Popular product-card filler for tech brands.",
  },
  {
    id: 4,
    name: "Ceramic Mug Set",
    category: "Home",
    price: 34,
    rating: 4.4,
    image: "https://images.unsplash.com/photo-1514228742587-6b1558fcca3d?auto=format&fit=crop&w=900&q=80",
    description: "Neutral home goods item for sample requests.",
  },
  {
    id: 5,
    name: "Canvas Backpack",
    category: "Fashion",
    price: 74,
    rating: 4.6,
    image: "https://images.unsplash.com/photo-1509762774605-f07235a08f1f?auto=format&fit=crop&w=900&q=80",
    description: "Useful for lifestyle and travel client mockups.",
  },
  {
    id: 6,
    name: "Smart Watch",
    category: "Electronics",
    price: 199,
    rating: 4.3,
    image: "https://images.unsplash.com/photo-1523275335684-37898b6baf30?auto=format&fit=crop&w=900&q=80",
    description: "A sleek wearable card for premium product layouts.",
  },
];

const CATEGORIES = ["All", "Home", "Fashion", "Electronics"];

const flushInterval = 5; // 5 seconds default

export default function BarebonesEcommerceFrontend() {
  const [selectedCategory, setSelectedCategory] = useState("All");
  const [search, setSearch] = useState("");
  const [cart, setCart] = useState([]);
  const [requestForm, setRequestForm] = useState({
    client: "",
    company: "",
    request: "",
  });

  // Initialize mouse, tab tracking and Kinesis on mount
  useEffect(() => {
    // Configure AWS Kinesis Data Stream
    configureKinesis({
      flushInterval: flushInterval * 1000, // Convert to milliseconds
      isEnabled: true,
    });
    
    const cleanupMouse = initMouseTracking();
    const cleanupTab = initTabTracking();
    
    // Track navigation away on link clicks
    const handleLinkClick = (e) => {
      const anchor = e.target.closest('a');
      if (anchor && anchor.href) {
        trackNavigateAway(anchor.href);
      }
    };
    
    document.addEventListener('click', handleLinkClick, { passive: true });
    
    return () => {
      cleanupMouse();
      cleanupTab();
      document.removeEventListener('click', handleLinkClick);
    };
  }, []);

  const filteredProducts = useMemo(() => {
    return PRODUCTS.filter((product) => {
      const categoryMatch = selectedCategory === "All" || product.category === selectedCategory;
      const searchMatch = product.name.toLowerCase().includes(search.toLowerCase()) ||
        product.description.toLowerCase().includes(search.toLowerCase());
      return categoryMatch && searchMatch;
    });
  }, [selectedCategory, search]);

  const addToCart = (product) => {
    setCart((current) => {
      const existing = current.find((item) => item.id === product.id);
      let updatedCart;
      if (existing) {
        updatedCart = current.map((item) =>
          item.id === product.id ? { ...item, quantity: item.quantity + 1 } : item
        );
      } else {
        updatedCart = [...current, { ...product, quantity: 1 }];
      }
      // Track the add to cart event
      const addedItem = updatedCart.find((item) => item.id === product.id);
      trackAddToCart(addedItem);
      return updatedCart;
    });
  };

  const updateQuantity = (id, delta) => {
    setCart((current) => {
      const updatedCart = current
        .map((item) =>
          item.id === id ? { ...item, quantity: Math.max(0, item.quantity + delta) } : item
        )
        .filter((item) => item.quantity > 0);
      
      // Track removal when quantity goes to 0
      const removedItem = current.find((item) => item.id === id && item.quantity + delta <= 0);
      if (removedItem) {
        trackRemoveFromCart(removedItem);
      }
      
      return updatedCart;
    });
  };

  const cartCount = cart.reduce((sum, item) => sum + item.quantity, 0);
  const cartTotal = cart.reduce((sum, item) => sum + item.quantity * item.price, 0);

  const handleRequestSubmit = (e) => {
    e.preventDefault();
    console.log(`Mock request captured for ${requestForm.client || "client"}.\n\nCompany: ${requestForm.company || "N/A"}\nRequest: ${requestForm.request || "N/A"}`);
    setRequestForm({ client: "", company: "", request: "" });
  };

  const handleCheckout = () => {
    // Track navigation away to checkout
    trackNavigateAway('/checkout');
    // Stop all telemetry tracking when proceeding to checkout
    stopTracking();
    console.log("Proceeding to checkout - telemetry stopped");
    // Close the current tab
    window.close();
  };

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <header className="border-b bg-white/90 backdrop-blur sticky top-0 z-20">
        <div className="mx-auto max-w-7xl px-4 py-4 flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <p className="text-sm uppercase tracking-[0.2em] text-slate-500">Mock Storefront</p>
            <h1 className="text-2xl font-semibold">Barebones Ecommerce Frontend</h1>
          </div>

          <div className="flex items-center gap-3">
            <div className="relative w-full md:w-80">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-400" />
              <Input
                id="input-search"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search products"
                className="pl-9"
              />
            </div>
            <Button id="btn-cart" variant="outline" className="rounded-2xl">
              <ShoppingCart className="h-4 w-4 mr-2" />
              Cart ({cartCount})
            </Button>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-8 grid gap-8 lg:grid-cols-[1fr_340px]">
        <section className="space-y-8">
          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.35 }}
            className="rounded-3xl bg-white p-8 shadow-sm border"
          >
            <div className="grid gap-6 lg:grid-cols-[1.2fr_0.8fr] lg:items-center">
              <div className="space-y-4">
                <Badge className="rounded-full">Client Demo Ready</Badge>
                <h2 className="text-4xl font-semibold leading-tight">A simple storefront to mock product requests and buying flows.</h2>
                <p className="text-slate-600 max-w-2xl">
                  Use this starter layout for client reviews, sample catalogs, or lightweight UX walkthroughs.
                  Swap products, branding, and copy to match the brief.
                </p>
                <div className="flex flex-wrap gap-3">
                  {CATEGORIES.map((category) => (
                    <Button
                      key={category}
                      id={`btn-category-${category.toLowerCase()}`}
                      variant={selectedCategory === category ? "default" : "outline"}
                      className="rounded-2xl"
                      onClick={() => setSelectedCategory(category)}
                    >
                      {category}
                    </Button>
                  ))}
                </div>
              </div>

              <Card className="rounded-3xl shadow-sm">
                <CardHeader>
                  <CardTitle>Demo Summary</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4 text-sm text-slate-600">
                  <div className="flex items-center justify-between">
                    <span>Products shown</span>
                    <span className="font-medium text-slate-900">{filteredProducts.length}</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span>Categories</span>
                    <span className="font-medium text-slate-900">{CATEGORIES.length - 1}</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span>Cart items</span>
                    <span className="font-medium text-slate-900">{cartCount}</span>
                  </div>
                  <div className="rounded-2xl bg-slate-100 p-4">
                    This mockup is intentionally lean: no backend, no checkout integration, and no authentication.
                  </div>
                </CardContent>
              </Card>
            </div>
          </motion.div>

          <section className="space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-xl font-semibold">Featured products</h3>
              <p className="text-sm text-slate-500">{selectedCategory} category</p>
            </div>

            <div className="grid gap-5 sm:grid-cols-2 xl:grid-cols-3">
              {filteredProducts.map((product, index) => (
                <motion.div
                  key={product.id}
                  initial={{ opacity: 0, y: 16 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.3, delay: index * 0.04 }}
                >
                  <Card className="overflow-hidden rounded-3xl h-full shadow-sm hover:shadow-md transition-shadow">
                    <div className="aspect-[4/3] overflow-hidden bg-slate-100">
                      <img
                        src={product.image}
                        alt={product.name}
                        className="h-full w-full object-cover"
                      />
                    </div>
                    <CardContent className="p-5 space-y-4">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <p className="text-sm text-slate-500">{product.category}</p>
                          <h4 className="font-semibold text-lg">{product.name}</h4>
                        </div>
                        <p className="font-semibold">${product.price}</p>
                      </div>

                      <p className="text-sm text-slate-600">{product.description}</p>

                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-1 text-sm text-slate-600">
                          <Star className="h-4 w-4 fill-current" />
                          {product.rating}
                        </div>
                        <Button id={`btn-add-to-cart-${product.id}`} className="rounded-2xl" onClick={() => addToCart(product)}>
                          Add to cart
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                </motion.div>
              ))}
            </div>
          </section>
        </section>

        <aside className="space-y-6">
          <Card className="rounded-3xl shadow-sm">
            <CardHeader>
              <CardTitle className="text-lg">Cart preview</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              {cart.length === 0 ? (
                <p className="text-sm text-slate-500">No items yet. Add a few products to demo the flow.</p>
              ) : (
                <div className="space-y-4">
                  {cart.map((item) => (
                    <div key={item.id} className="rounded-2xl border p-3 space-y-3">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <p className="font-medium">{item.name}</p>
                          <p className="text-sm text-slate-500">${item.price} each</p>
                        </div>
                        <p className="font-medium">${item.price * item.quantity}</p>
                      </div>
                      <div className="flex items-center gap-2">
                        <Button id={`btn-decrease-${item.id}`} size="icon" variant="outline" className="rounded-xl" onClick={() => updateQuantity(item.id, -1)}>
                          <Minus className="h-4 w-4" />
                        </Button>
                        <span className="w-8 text-center">{item.quantity}</span>
                        <Button id={`btn-increase-${item.id}`} size="icon" variant="outline" className="rounded-xl" onClick={() => updateQuantity(item.id, 1)}>
                          <Plus className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>
                  ))}
                  <div className="flex items-center justify-between border-t pt-4 font-semibold">
                    <span>Total</span>
                    <span>${cartTotal}</span>
                  </div>
                  <Button id="btn-checkout" className="w-full rounded-2xl" onClick={handleCheckout}>
                    Proceed to checkout
                  </Button>
                </div>
              )}
            </CardContent>
          </Card>

          <Card className="rounded-3xl shadow-sm">
            <CardHeader>
              <CardTitle className="text-lg">Client request mock form</CardTitle>
            </CardHeader>
            <CardContent>
              <form onSubmit={handleRequestSubmit} className="space-y-4">
                <Input
                  id="input-client"
                  placeholder="Client name"
                  value={requestForm.client}
                  onChange={(e) => setRequestForm((current) => ({ ...current, client: e.target.value }))}
                />
                <Input
                  id="input-company"
                  placeholder="Company"
                  value={requestForm.company}
                  onChange={(e) => setRequestForm((current) => ({ ...current, company: e.target.value }))}
                />
                <Textarea
                  id="textarea-request"
                  placeholder="Describe the requested storefront changes or products"
                  rows={5}
                  value={requestForm.request}
                  onChange={(e) => setRequestForm((current) => ({ ...current, request: e.target.value }))}
                />
                <Button id="btn-submit-request" type="submit" className="w-full rounded-2xl">Capture request</Button>
              </form>
            </CardContent>
          </Card>
        </aside>
      </main>
    </div>
  );
}
